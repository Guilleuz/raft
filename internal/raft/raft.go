// Escribir vuestro código de funcionalidad Raft en este fichero
//

package raft

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"raft/internal/comun/rpctimeout"
	"runtime"
	"sync"
	"time"
)

//  false deshabilita por completo los logs de depuracion
// Aseguraros de poner kEnableDebugLogs a false antes de la entrega
const kEnableDebugLogs = true

// Poner a true para logear a stdout en lugar de a fichero
const kLogToStdout = true

// Cambiar esto para salida de logs en un directorio diferente
const kLogOutputDir = "./logs_raft/"

// Definición de estados

const SEGUIDOR = 0

const CANDIDATO = 1

const LIDER = 2

func checkError(err error) {
	if err != nil {
		_, _, linea, _ := runtime.Caller(1)
		fmt.Fprintf(os.Stderr, "Fatal error: %s\n", err.Error())
		fmt.Fprintf(os.Stderr, "Línea: %d\n", linea)
	}
}

// A medida que el nodo Raft conoce las operaciones de las  entradas de registro
// comprometidas, envía un AplicaOperacion, con cada una de ellas, al canal
// "canalAplicar" (funcion NuevoNodo) de la maquina de estados
type AplicaOperacion struct {
	indice    int // en la entrada de registro
	operacion interface{}
}

// Tipo de dato Go que representa un solo nodo (réplica) de raft
//

type Operacion struct {
	Mandato   int
	Operacion interface{}
}

type NodoRaft struct {
	mux sync.Mutex // Mutex para proteger acceso a estado compartido

	nodos []string // Conexiones RPC a todos los nodos (réplicas) Raft
	yo    int      // this peer's index into peers[]
	// Utilización opcional de este logger para depuración
	// Cada nodo Raft tiene su propio registro de trazas (logs)
	logger *log.Logger
	estado int // Estado en el que el nodo cree estar

	// Vuestros datos aqui.
	// mirar figura 2 para descripción del estado que debe mantenre un nodo Raft

	currentTerm int         // mandato actual
	votedFor    int         // id del nodo al que votó en el mandato actual, nulo si no votó
	log         []Operacion // registro de entradas

	commitIndex int // indice de la ultima operacion comprometida que conozcamos
	lastApplied int // indice de la ultima operacion que hemos aplicado a la máquina de estados

	// Estado del líder
	nextIndex  []int // indice del siguiente registro de entradas a mandar
	matchIndex []int // indice del mayor registro de entradas conocido para ser replicado

	mensajeLatido chan bool
	canalStop     chan bool
}

func (nr *NodoRaft) gestionEstado() {
	nr.logger.Printf("Réplica %d: comienza la ejecución\n", nr.yo)
	rand.Seed(time.Now().UnixNano())
	for {
		select {
		case <-nr.canalStop:
			nr.logger.Printf("Réplica %d: finaliza la ejecución\n", nr.yo)
			return
		default:
			nr.mux.Lock()
			estadoActual := nr.estado
			nr.mux.Unlock()
			switch estadoActual {
			case SEGUIDOR:
				// Definimos un timeout aleatorio entre 150 y 300 ms
				timeout := time.After(time.Duration(rand.Intn(151)+150) * time.Millisecond)
				select {
				case <-nr.mensajeLatido:
					// Seguimos en seguidor, se reinicia el timeout
				case <-timeout:
					// Timeout expirado, pasamos a candidato
					nr.mux.Lock()
					nr.estado = CANDIDATO
					nr.mux.Unlock()
				}
			case CANDIDATO:
				nr.eleccion()
			case LIDER:
				// latido 20 veces por segundo (cada 50 ms)
				nr.logger.Printf("Réplica %d: envío latido, mandato: %d\n", nr.yo, nr.currentTerm)
				nr.AppendEntries([]Operacion{}, 50*time.Millisecond)
			}
		}
	}
}

func NuevoNodo(nodos []string, yo int, canalAplicar chan AplicaOperacion) *NodoRaft {
	nr := &NodoRaft{}
	nr.nodos = nodos
	for i, nodo := range nodos {
		fmt.Printf("Nodo %d: %s\n", i, nodo)
	}
	nr.yo = yo
	nr.mux = sync.Mutex{}
	nr.mensajeLatido = make(chan bool, 100)
	nr.canalStop = make(chan bool)
	nr.votedFor = -1
	nr.log = []Operacion{{0, nil}}
	nr.estado = SEGUIDOR
	nr.nextIndex = make([]int, len(nodos))
	nr.matchIndex = make([]int, len(nodos))

	if kEnableDebugLogs {
		nombreNodo := nodos[yo]
		logPrefix := fmt.Sprintf("%s ", nombreNodo)
		if kLogToStdout {
			nr.logger = log.New(os.Stdout, nombreNodo,
				log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt",
				kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			nr.logger = log.New(logOutputFile, logPrefix, log.Lmicroseconds|log.Lshortfile)
		}
		nr.logger.Println("logger initialized")
	} else {
		nr.logger = log.New(ioutil.Discard, "", 0)
	}

	go nr.gestionEstado()
	return nr
}

func (nr *NodoRaft) eleccion() {
	// Petición de voto
	var peticion ArgsPeticionVoto
	nr.mux.Lock()
	nr.votedFor = nr.yo
	peticion.CandidateId = nr.yo
	peticion.LastLogIndex = len(nr.log) - 1
	peticion.LastLogTerm = (nr.log[len(nr.log)-1]).Mandato
	// Incrementamos el mandato
	nr.currentTerm++
	peticion.Term = nr.currentTerm
	nr.mux.Unlock()
	nr.logger.Printf("Réplica %d: comienzo una elección, mandato: %d\n", nr.yo, nr.currentTerm)

	votosRecibidos := 1
	// Timeout aleatorio entre 100 y 200 ms
	timeout := time.After(time.Duration(rand.Intn(101)+100) * time.Millisecond)

	canalVoto := make(chan bool, len(nr.nodos))
	canalMandato := make(chan int, len(nr.nodos))

	for i, _ := range nr.nodos {
		if i != nr.yo {
			// Por cada réplica, mandamos una petición de voto
			go nr.gestionarPeticionVoto(i, canalVoto, canalMandato, peticion)

		}
	}

	select {
	// Recibimos las respuestas a las peticiones de voto
	case voto := <-canalVoto:
		if voto {
			// Si recibimos un voto, y tenemos mayoría, la elección
			// acaba y pasamos a ser el líder
			votosRecibidos++
			if votosRecibidos >= len(nr.nodos)/2+1 {
				nr.mux.Lock()
				nr.estado = LIDER
				for i := 0; i < len(nr.nodos); i++ {
					nr.nextIndex[i] = len(nr.log)
					nr.matchIndex[i] = 0
				}
				nr.mux.Unlock()
				nr.logger.Printf("Réplica %d: he recibido mayoría, paso a LIDER\n", nr.yo)
			}
		}
	case mandato := <-canalMandato:
		// Si recibimos una respuesta con mayor mandato que
		// el nuestro, pasamos a ser seguidor
		if mandato > nr.currentTerm {
			nr.mux.Lock()
			nr.votedFor = -1
			nr.currentTerm = mandato
			nr.estado = SEGUIDOR
			nr.mux.Unlock()
		}
		nr.logger.Printf("Réplica %d: (candidato) mandato superior encontrado, paso a SEGUIDOR\n", nr.yo)
	case <-timeout:
		// Si ha expirado el timeout, y no hemos conseguido
		// la mayoría, ni hemos encontrado a alguien con mayor mandato,
		// empezamos una nueva elección
		nr.logger.Printf("Réplica %d: (candidato) timeout eleccion, mandato: %d\n", nr.yo, nr.currentTerm)
	}
	fmt.Println("Eleccion finalizada")
}

// Metodo Para() utilizado cuando no se necesita mas al nodo
//
// Quizas interesante desactivar la salida de depuracion
// de este nodo
//
func (nr *NodoRaft) Para() {
	nr.canalStop <- true
}

// Devuelve "yo", mandato en curso y si este nodo cree ser lider
//
func (nr *NodoRaft) ObtenerEstado() (int, int, bool) {
	return nr.yo, nr.currentTerm, nr.estado == LIDER
}

func (nr *NodoRaft) SometerOperacion(operacion interface{}) (int, int, bool) {
	nr.mux.Lock()
	indice := len(nr.log)
	mandato := nr.currentTerm
	EsLider := nr.estado == LIDER
	nr.mux.Unlock()

	if EsLider {
		nr.mux.Lock()
		nr.log = append(nr.log, Operacion{nr.currentTerm, operacion})
		go nr.AppendEntries([]Operacion{{nr.currentTerm, operacion}}, 50*time.Millisecond)
		nr.mux.Unlock()
		nr.logger.Printf("Réplica %d: (lider) recibo una nueva operación, mandato: %d\n", nr.yo, nr.currentTerm)
	}

	return indice, mandato, EsLider
}

type ArgsPeticionVoto struct {
	// Argumentos
	Term         int // mandato del candidato
	CandidateId  int // id del candidato
	LastLogIndex int // indice de la última entrada del registro del candidato
	LastLogTerm  int // mandato de la última entrada del registro del candidato
}

type RespuestaPeticionVoto struct {
	Term        int  // Mandato actual
	VoteGranted bool // True si le concede el voto al candidato, false si no
}

func (nr *NodoRaft) PedirVoto(args *ArgsPeticionVoto, reply *RespuestaPeticionVoto) error {
	nr.mux.Lock()
	nr.logger.Printf("Replica %d: peticion voto de %d, he votado a %d, mi mandato: %d, el del candidato: %d\n", nr.yo, args.CandidateId, nr.votedFor, nr.currentTerm, args.Term)
	if nr.votedFor == -1 || nr.votedFor == args.CandidateId || args.Term > nr.currentTerm {
		nr.votedFor = args.CandidateId
		nr.currentTerm = args.Term
		nr.estado = SEGUIDOR
		nr.mux.Unlock()
		nr.logger.Printf("Réplica %d: le concedo el voto al candidato %d\n", nr.yo, args.CandidateId)
		reply.Term = args.Term
		reply.VoteGranted = true
		nr.mensajeLatido <- true

	} else {
		nr.mux.Unlock()
		reply.VoteGranted = false
		reply.Term = nr.currentTerm
		nr.logger.Printf("Réplica %d: le niego el voto al candidato %d\n", nr.yo, args.CandidateId)
	}

	return nil
}

func (nr *NodoRaft) enviarPeticionVoto(nodo int, args *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) bool {
	cliente, err := rpc.DialHTTP("tcp", nr.nodos[nodo])
	checkError(err)
	if cliente != nil {
		nr.logger.Printf("Réplica %d: (candidato) le pido el voto a %d\n", nr.yo, nodo)
		err = rpctimeout.CallTimeout(cliente, "NodoRaft.PedirVoto", args, reply, 90*time.Millisecond)
		cliente.Close()
		checkError(err)
	} else {
		fmt.Println("Cliente es nulo")
	}
	return err == nil
}

func (nr *NodoRaft) gestionarPeticionVoto(nodo int, canalVoto chan bool, canalMandato chan int,
	args ArgsPeticionVoto) {
	// Las goroutinas comparten memoria, la variable i cambia antes de ejecutar la peticion
	// Solucion -> go funcion(int i)
	var respuesta *RespuestaPeticionVoto = new(RespuestaPeticionVoto)
	ok := nr.enviarPeticionVoto(nodo, &args, respuesta)
	if ok {
		canalVoto <- respuesta.VoteGranted
		canalMandato <- respuesta.Term
	}
}

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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (nr *NodoRaft) AppendEntry(args *AppendEntryPeticion, reply *AppendEntryRespuesta) error {
	if nr.estado == SEGUIDOR {
		// Reiniciamos el timeout del seguidor
		nr.mensajeLatido <- true
	}
	nr.mux.Lock()
	if nr.currentTerm < args.Term && nr.estado == CANDIDATO {
		// Si el mandato es mayor o igual que el mío, paso a seguidor
		nr.currentTerm = args.Term
		nr.votedFor = -1
		nr.estado = SEGUIDOR
		nr.logger.Printf("Réplica %d: (candidato) AppendEntry de %d rechazado, mi mandato: %d, el suyo: %d\n", nr.yo, args.LeaderId, nr.currentTerm, args.Term)
		reply.Term = nr.currentTerm
		reply.Success = false
		nr.mux.Unlock()
		return nil
	}

	nr.mux.Unlock()
	nr.logger.Printf("Réplica %d: %d entrada(s) de log recibidas de %d\n", nr.yo, len(args.Entries), args.LeaderId)
	nr.logger.Printf("Réplica %d: Entires recibido:\n", nr.yo)
	for _, entry := range args.Entries {
		nr.logger.Println(entry)
	}
	reply.Term = nr.currentTerm
	if nr.currentTerm > args.Term || len(nr.log) <= args.PrevLogIndex || nr.log[args.PrevLogIndex].Mandato != args.PrevLogTerm {
		// Si mi mandato es mayor, o el log no contiene una entrada en PrevLogIndex con el mandato PrevLogTerm
		// Success será falso
		reply.Success = false
	} else {
		// Si no, success será true
		reply.Success = true

		nr.mux.Lock()
		nr.log = append(nr.log, args.Entries...)

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
	cliente, err := rpc.DialHTTP("tcp", nr.nodos[nodo])
	checkError(err)
	if cliente != nil {
		nr.logger.Printf("Réplica %d: (lider) %d entrada(s) de log enviadas a %d\n", nr.yo, len(args.Entries), nodo)
		err = rpctimeout.CallTimeout(cliente, "NodoRaft.AppendEntry", &args, &reply, 90*time.Millisecond)
		cliente.Close()
		checkError(err)
	}
	return err == nil
}

func (nr *NodoRaft) AppendEntries(entries []Operacion, timeout time.Duration) {
	// Petición de AppendEntry
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
		peticion.PrevLogTerm = nr.log[peticion.PrevLogTerm].Mandato
	}
	nr.mux.Unlock()

	timeoutChan := time.After(timeout)
	canalMandato := make(chan int, len(nr.nodos))

	for i := 0; i < len(nr.nodos); i++ {
		if i != nr.yo {
			// Por cada réplica, mandamos una petición de Append Entry
			go nr.gestionarEnvioAppendEntry(i, peticion, canalMandato)
		}
	}

	select {
	// Recibimos las respuestas a las peticiones de voto
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
		}
	case <-timeoutChan:
		// Si vence el timeout, dejamos de esperar las respuestas
	}

	// Actualizamos el commitIndex
	nr.mux.Lock()
	nr.commitIndex += len(entries)
	nr.mux.Unlock()
}

func (nr *NodoRaft) gestionarEnvioAppendEntry(nodo int, args AppendEntryPeticion, canalMandato chan int) {
	var respuesta AppendEntryRespuesta
	ok := nr.enviarAppendEntry(nodo, &args, &respuesta)
	if ok {
		canalMandato <- respuesta.Term
	}
}
