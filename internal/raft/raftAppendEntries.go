package raft

import (
	"net/rpc"
	"raft/internal/comun/rpctimeout"
	"strconv"
	"time"
)

type AppendEntryPeticion struct {
	Term         int // mandato del líder
	LeaderId     int // id del líder
	PrevLogIndex int // índice del registro de entradas que precede a las nuevas

	PrevLogTerm int         // mandato de la entrada 'PrevLogIndex'
	Entries     []Operacion // registro de entradas a guardar (vacío para latido)

	LeaderCommit int // Indice de la ultima operación comprometida del líder

}

type AppendEntryRespuesta struct {
	Term    int  // mandato actual
	Success bool // true si contiene una entrada que coincida con PrevLogIndex y PrevLogTerm
}

// Devuelve el mínimo de a y b
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Llamada RPC AppendEntry, que permite añadir una serie de entradas al log de una réplica
func (nr *NodoRaft) AppendEntry(args *AppendEntryPeticion, reply *AppendEntryRespuesta) error {
	nr.mux.Lock()
	if nr.currentTerm < args.Term {
		// Si el mandato es mayor que el mío, lo actualizo y paso a seguidor
		nr.currentTerm = args.Term
		nr.votedFor = -1
		nr.estado = SEGUIDOR
	}
	nr.mux.Unlock()

	if nr.estado == SEGUIDOR {
		// Reiniciamos el timeout del seguidor
		nr.mensajeLatido <- true
	}

	reply.Term = nr.currentTerm
	// TODO -> ignoro si mi mandato es mayor o igual y soy candidato o lider
	// TODO -> parece haber fallo al aceptar entradas
	nr.logger.Printf("Réplica %d: AppendEntry de %d, len de mi log: %d, PrevLogIndex: %d, mandato ultima entrada: %d, PrevLogTerm: %d\n",
		nr.yo, args.LeaderId, len(nr.log), args.PrevLogIndex, nr.log[len(nr.log)-1].Mandato, args.PrevLogTerm)

	if nr.currentTerm > args.Term || (nr.currentTerm >= args.Term && (nr.estado == CANDIDATO || nr.estado == LIDER)) ||
		len(nr.log) <= args.PrevLogIndex || nr.log[args.PrevLogIndex].Mandato != args.PrevLogTerm {
		// Si mi mandato es mayor, o el log no contiene una entrada
		// en PrevLogIndex con el mandato PrevLogTerm
		// Success será falso
		nr.logger.Printf("Réplica %d: %d entrada(s) de log ignoradas de %d\n",
			nr.yo, len(args.Entries), args.LeaderId)
		reply.Success = false
	} else {
		// Si no, success será true
		reply.Success = true
		nr.logger.Printf("Réplica %d: %d entrada(s) de log aceptadas de %d\n",
			nr.yo, len(args.Entries), args.LeaderId)

		nr.mux.Lock()
		// Actualizamos el lider
		nr.lider = args.LeaderId
		for i := 0; i < len(args.Entries); i++ {
			// Por cada una de las nuevas entradas
			if args.PrevLogIndex+1+i >= len(nr.log) {
				// Si el log está vacío en esa posición, añadimos las nuevas entradas restantes
				nr.log = append(nr.log, args.Entries[i:]...)
				break
			} else if nr.log[args.PrevLogIndex+1+i].Mandato != args.Entries[i].Mandato {
				// Si el log no coincide con la entrada correspondiente,
				// eliminamos esa entrada y las posteriores, sustituyéndolas por
				// las nuevas entradas restantes
				nr.log = append(nr.log[0:args.PrevLogIndex+1+i], args.Entries[i:]...)
				break
			}
		}

		// Actualizamos nuestro commitIndex
		if args.LeaderCommit > nr.commitIndex {
			nr.commitIndex = min(args.LeaderCommit, len(nr.log)-1)
			nr.logger.Printf("Réplica %d: (seguidor) entrada %d comprometida", nr.yo, nr.commitIndex)
		}
		nr.mux.Unlock()
	}

	// Comprometemos entradas si fuera posible
	nr.comprometerEntradas()

	var cadenaLog string = ""
	nr.mux.Lock()
	for i, entrada := range nr.log {
		cadenaLog = cadenaLog + strconv.Itoa(i) + ":" + strconv.Itoa(entrada.Mandato) + " "
	}
	nr.mux.Unlock()
	nr.logger.Printf("Réplica %d, mi log: %s", nr.yo, cadenaLog)
	return nil
}

// Realiza un AppendEntry a la réplica "nodo"
// Devuelve true si ha recibido respuesta antes de vencer el timeout
// Devuelve false en caso contrario
func (nr *NodoRaft) enviarAppendEntry(nodo int, args *AppendEntryPeticion,
	reply *AppendEntryRespuesta) bool {
	cliente, err := rpc.Dial("tcp", nr.nodos[nodo])
	checkError(err)
	if cliente != nil {
		nr.logger.Printf("Réplica %d: (lider) %d entrada(s) de log enviadas a %d\n",
			nr.yo, len(args.Entries), nodo)
		err = rpctimeout.CallTimeout(cliente, "NodoRaft.AppendEntry",
			&args, &reply, 90*time.Millisecond)
		cliente.Close()
		checkError(err)
	}
	return err == nil
}

// Función que gestiona una llamada a AppendEntry, enviando por el canal "canalMandato", el mandato de
// la réplica a la que se envío el AppendEntry
func (nr *NodoRaft) gestionarEnvioAppendEntry(nodo int, args AppendEntryPeticion, canalMandato chan int, canalSuccess chan Success) {
	var respuesta AppendEntryRespuesta
	ok := nr.enviarAppendEntry(nodo, &args, &respuesta)
	if ok {
		canalMandato <- respuesta.Term
		canalSuccess <- Success{respuesta.Success, nodo}
	}
}

type Success struct {
	success bool
	idNodo  int
}

// Envío de llamada AppendEntry a todas las réplicas
func (nr *NodoRaft) AppendEntries(timeout time.Duration) {
	// Inicializamos la petición de AppendEntry
	var peticion AppendEntryPeticion
	nr.mux.Lock()
	peticion.Term = nr.currentTerm
	peticion.LeaderId = nr.yo
	peticion.LeaderCommit = nr.commitIndex
	nuevoNext := len(nr.log)
	nuevoMatch := len(nr.log) - 1
	nr.mux.Unlock()

	canalMandato := make(chan int, len(nr.nodos))
	canalSuccess := make(chan Success, len(nr.nodos))

	for i := 0; i < len(nr.nodos); i++ {
		if i != nr.yo {
			// Por cada réplica, mandamos una petición de Append Entry
			nr.mux.Lock()
			peticion.PrevLogIndex = nr.nextIndex[i] - 1
			peticion.PrevLogTerm = nr.log[peticion.PrevLogIndex].Mandato
			peticion.Entries = nr.log[nr.nextIndex[i]:]
			nr.mux.Unlock()
			go nr.gestionarEnvioAppendEntry(i, peticion, canalMandato, canalSuccess)
		}
	}

	nr.gestionRespuestas(timeout, canalMandato, canalSuccess, nuevoNext, nuevoMatch)

	// Actualizamos el commitIndex
	nr.actualizarCommitIndex()

	// Comprometemos entradas en caso de que fuera posible
	nr.comprometerEntradas()
}

// Gestión de las respuestas a AppendEntry recibidas de los nodo
func (nr *NodoRaft) gestionRespuestas(timeout time.Duration, canalMandato chan int, canalSuccess chan Success, nuevoNextIndex int, nuevoMatchIndex int) {
	timeoutChannel := time.After(timeout)
	for {
		select {
		// Recibimos las respuestas a las llamadas
		case respuesta := <-canalSuccess:
			if respuesta.success {
				// Si succes es true, actualizamos matchIndex y nextIndex
				nr.mux.Lock()
				nr.nextIndex[respuesta.idNodo] = nuevoNextIndex
				nr.matchIndex[respuesta.idNodo] = nuevoMatchIndex
				nr.mux.Unlock()
			} else {
				// Si success es falso, decrementamos el nextIndex
				nr.mux.Lock()
				if nr.nextIndex[respuesta.idNodo] > 1 {
					nr.nextIndex[respuesta.idNodo]--
				}
				nr.mux.Unlock()
			}
		case mandato := <-canalMandato:
			// Si recibimos una respuesta con mayor mandato que
			// el nuestro, pasamos a ser seguidor
			if mandato > nr.currentTerm {
				nr.mux.Lock()
				nr.currentTerm = mandato
				nr.votedFor = -1
				nr.estado = SEGUIDOR
				nr.mux.Unlock()
				nr.logger.Printf("Réplica %d: (lider) mandato superior encontrado, paso a SEGUIDOR\n", nr.yo)
				return
			}
		case <-timeoutChannel:
			// Si vence el timeout, dejamos de esperar las respuestas
			return
		}
	}
}

func (nr *NodoRaft) actualizarCommitIndex() {
	// Actualizamos el commitIndex
	nr.mux.Lock()
	for n := nr.commitIndex + 1; n < len(nr.log); n++ {

		// Solo comprometemos entradas de nuestro mandato
		if nr.log[n].Mandato == nr.currentTerm {
			total := 1 // La entrada ya esta en nuestro log
			for i := 0; i < len(nr.nodos); i++ {
				if nr.matchIndex[i] >= n {
					total++
				}
			}

			// Si una mayoría tiene un match index mayor o igual que n
			// La entrada n y todas las anteriores estarán comprometidas
			if total >= len(nr.nodos)/2+1 {
				nr.commitIndex = n
				//nr.logger.Printf("Réplica %d: (lider) entrada %d comprometida", nr.yo, nr.commitIndex)
			} else {
				// Si la primera entrada de ese mandato no se puede comprometer, tampoco
				// podremos comprometer entradas posteriores
				break
			}
		}
	}
	nr.mux.Unlock()
}

func (nr *NodoRaft) comprometerEntradas() {
	nr.mux.Lock()
	for i := nr.lastApplied + 1; i <= nr.commitIndex; i++ {
		canalRespuesta := make(chan string)
		nr.canalAplicar <- AplicaOperacion{canalRespuesta, i, nr.log[i].Operacion}
		respuesta := <-canalRespuesta
		nr.logger.Printf("Réplica %d: comprometo la entrada %d", nr.yo, i)
		// mandar a la rutina que trata al cliente
		for n, j := range nr.respuestas {
			if j.indice == i {
				nr.logger.Printf("Réplica %d: comunico la respuesta de comprometer la entrada %d", nr.yo, i)
				j.canalRespuesta <- respuesta
				nr.respuestas = append(nr.respuestas[0:n], nr.respuestas[n+1:]...)
				break
			}
		}
	}
	nr.lastApplied = nr.commitIndex
	nr.mux.Unlock()
}
