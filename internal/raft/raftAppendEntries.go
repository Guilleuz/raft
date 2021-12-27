package raft

import (
	"net/rpc"
	"raft/internal/comun/rpctimeout"
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
	if nr.currentTerm < args.Term || mejor log{
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
	nr.logger.Printf("Réplica %d: AppendEntry de %d, len de mi log: %d, PrevLogIndex: %d, mandato PrevLogIndex: %d, PrevLogTerm: %d\n",
			nr.yo, args.LeaderId, len(nr.log), args.PrevLogIndex, nr.log[args.PrevLogIndex].Mandato, args.PrevLogTerm )
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
		// Actualizamos el log
		nr.log = append(nr.log, args.Entries...)
		// Actualizamos el lider
		nr.lider = args.LeaderId

		// Actualizamos nuestro commitIndex
		if args.LeaderCommit > nr.commitIndex {
			nr.commitIndex = min(args.LeaderCommit, len(nr.log)-1)
		}
		nr.mux.Unlock()
	}

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
func (nr *NodoRaft) gestionarEnvioAppendEntry(nodo int, args AppendEntryPeticion, canalMandato chan int) {
	var respuesta AppendEntryRespuesta
	ok := nr.enviarAppendEntry(nodo, &args, &respuesta)
	if ok {
		canalMandato <- respuesta.Term
	}
}

// Envío de llamada AppendEntry a todas las réplicas
func (nr *NodoRaft) AppendEntries(entries []Operacion, timeout time.Duration) {
	// Inicializamos la petición de AppendEntry
	var peticion AppendEntryPeticion
	nr.mux.Lock()
	peticion.Term = nr.currentTerm
	peticion.LeaderId = nr.yo
	peticion.LeaderCommit = nr.commitIndex
	peticion.Entries = entries
	peticion.PrevLogIndex = len(nr.log) - 1 - len(entries)
	if peticion.PrevLogIndex < 0 {
		peticion.PrevLogIndex = 0
		peticion.PrevLogTerm = 0
	} else {
		peticion.PrevLogTerm = nr.log[peticion.PrevLogIndex].Mandato
	}
	nr.mux.Unlock()

	canalMandato := make(chan int, len(nr.nodos))

	for i := 0; i < len(nr.nodos); i++ {
		if i != nr.yo {
			// Por cada réplica, mandamos una petición de Append Entry
			go nr.gestionarEnvioAppendEntry(i, peticion, canalMandato)
		}
	}

	nr.gestionRespuestas(timeout, canalMandato)

	// Actualizamos el commitIndex
	nr.mux.Lock()
	nr.commitIndex += len(entries)
	nr.mux.Unlock()
}

// Gestión de las respuestas a AppendEntry recibidas de los nodo
func (nr *NodoRaft) gestionRespuestas(timeout time.Duration, canalMandato chan int) {
	timeoutChannel := time.After(timeout)
	for {
		select {
		// Recibimos las respuestas a las llamadas
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
		case <- timeoutChannel:
			// Si vence el timeout, dejamos de esperar las respuestas
			return
		}
	}
}
