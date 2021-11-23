// Escribir vuestro código de funcionalidad Raft en este fichero
//

package raft

//
// API
// ===
// Este es el API que vuestra implementación debe exportar
//
// nodoRaft = NuevoNodo(...)
//   Crear un nuevo servidor del grupo de elección.
//
// nodoRaft.Para()
//   Solicitar la parado de un servidor
//
// nodo.ObtenerEstado() (yo, mandato, esLider)
//   Solicitar a un nodo de elección por "yo", su mandato en curso,
//   y si piensa que es el msmo el lider
//
// nodoRaft.SometerOperacion(operacion interface()) (indice, mandato, esLider)

// type AplicaOperacion

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
		os.Exit(1)
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
}

// Creacion de un nuevo nodo de eleccion
//
// Tabla de <Direccion IP:puerto> de cada nodo incluido a si mismo.
//
// <Direccion IP:puerto> de este nodo esta en nodos[yo]
//
// Todos los arrays nodos[] de los nodos tienen el mismo orden

// canalAplicar es un canal donde, en la practica 5, se recogerán las
// operaciones a aplicar a la máquina de estados. Se puede asumir que
// este canal se consumira de forma continúa.
//
// NuevoNodo() debe devolver resultado rápido, por lo que se deberían
// poner en marcha Gorutinas para trabajos de larga duracion
func NuevoNodo(nodos []string, yo int, canalAplicar chan AplicaOperacion) *NodoRaft {
	nr := &NodoRaft{}
	nr.nodos = nodos
	nr.yo = yo
	nr.mux = sync.Mutex{}
	nr.mensajeLatido = make(chan bool, 100)
	nr.votedFor = -1

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

	// Your initialization code here (2A, 2B)

	// Necesaria una elección inicial
	// Cada nodo espera un timeout aleatorio a recibir un inicio de elección
	// Si no lo recibe, empieza el mismo la eleccion

	/*
		go routine
		for infinito
		switch ESTADO {
		case SEGUIDOR:
			select
			{
			case <- mensajeRecibido:
				reinicias el timeout -> valor aleatorio
			case <- timeout:
				pasamos CANDIDATO
			}
		case CANDIDATO:
			mandato++
			realizamos la eleccion
			select {
			case <- nodo con mandato mayor:
				pasamos a SEGUIDOR
			case <- mayoría:
				pasamos a LIDER
			case <- timeout
				seguimos en CANDIDATO
			}
		case LIDER:
			20 veces por segundo
			mandamos latido a todos los seguidores
			si descubrimos un nodo con mayor mandato -> pasamos a SEGUIDOR
		}
	*/

	go func() {
		rand.Seed(time.Now().UnixNano())
		for {
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
				// latido 20 veces por segundo
			}
		}
	}()

	/*
		LATIDO similar al proceso de eleccion
	*/
	return nr
}

/*
PROCESO DE ELECCION
tenemos canal respuestas
por cada nodo de la eleccion
	go func() {
		rpc.voto
		canal respuestas <- respuesta recibida
	}

esperamos en canal respuestas
select case, canal + timeout
*/
func (nr *NodoRaft) eleccion() {
	finEleccion := false

	// Petición de voto
	var peticion ArgsPeticionVoto
	nr.mux.Lock()
	nr.votedFor = nr.yo
	peticion.CandidateId = nr.yo
	peticion.LastLogIndex = len(nr.log) - 1
	peticion.LastLogTerm = (nr.log[len(nr.log)-1]).Mandato
	nr.mux.Unlock()

	// Realizamos elecciones hasta convertirnos en líder o
	// encontrar un nodo con mandato mayor al nuestro

	// TODO eliminar el bucle, que se realice con el bucle general
	// más fácil gestionar cambios de estado no debidos a la eleccion
	for !finEleccion {
		votosRecibidos := 1
		timeout := time.After(50 * time.Millisecond)

		// Incrementamos el mandato
		nr.mux.Lock()
		nr.currentTerm++
		peticion.Term = nr.currentTerm
		nr.mux.Unlock()
		canalVoto := make(chan bool, len(nr.nodos))
		canalMandato := make(chan int, len(nr.nodos))

		for i := 0; i < len(nr.nodos); i++ {
			if i != nr.yo {
				// Por cada réplica, mandamos una petición de voto
				go func() {
					var respuesta RespuestaPeticionVoto
					ok := nr.enviarPeticionVoto(i, &peticion, &respuesta)
					if ok {
						canalVoto <- respuesta.VoteGranted
						canalMandato <- respuesta.Term
					}
				}()
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
					finEleccion = true
					nr.mux.Lock()
					nr.estado = LIDER
					nr.mux.Unlock()
				}
			}
		case mandato := <-canalMandato:
			// Si recibimos una respuesta con mayor mandato que
			// el nuestro, pasamos a ser seguidor
			if mandato > nr.currentTerm {
				finEleccion = true
				nr.mux.Lock()
				nr.currentTerm = mandato
				nr.estado = SEGUIDOR
				nr.mux.Unlock()
			}
		case <-timeout:
			// Si ha expirado el timeout, y no hemos conseguido
			// la mayoría, ni hemos encontrado a alguien con mayor mandato,
			// empezamos una nueva elección
		}
	}
}

// Metodo Para() utilizado cuando no se necesita mas al nodo
//
// Quizas interesante desactivar la salida de depuracion
// de este nodo
//
func (nr *NodoRaft) Para() {

	// Vuestro codigo aqui

}

// Devuelve "yo", mandato en curso y si este nodo cree ser lider
//
func (nr *NodoRaft) ObtenerEstado() (int, int, bool) {
	var yo int
	var mandato int
	var esLider bool

	// Vuestro codigo aqui

	return yo, mandato, esLider
}

// El servicio que utilice Raft (base de datos clave/valor, por ejemplo)
// Quiere buscar un acuerdo de posicion en registro para siguiente operacion
// solicitada por cliente.

// Si el nodo no es el lider, devolver falso
// Sino, comenzar la operacion de consenso sobre la operacion y devolver con
// rapidez
//
// No hay garantia que esta operacion consiga comprometerse n una entrada de
// de registro, dado que el lider puede fallar y la entrada ser reemplazada
// en el futuro.
// Primer valor devuelto es el indice del registro donde se va a colocar
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
func (nr *NodoRaft) SometerOperacion(operacion interface{}) (int, int, bool) {
	indice := -1
	mandato := -1
	EsLider := true

	// Vuestro codigo aqui

	return indice, mandato, EsLider
}

//
// ArgsPeticionVoto
// ===============
//
// Structura de ejemplo de argumentos de RPC PedirVoto.
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
//
type ArgsPeticionVoto struct {
	// Argumentos
	Term         int // mandato del candidato
	CandidateId  int // id del candidato
	LastLogIndex int // indice de la última entrada del registro del candidato
	LastLogTerm  int // mandato de la última entrada del registro del candidato
}

//
// RespuestaPeticionVoto
// ================
//
// Structura de ejemplo de respuesta de RPC PedirVoto,
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
//
//
type RespuestaPeticionVoto struct {
	Term        int  // Mandato actual
	VoteGranted bool // True si le concede el voto al candidato, false si no
}

//
// PedirVoto
// ===========
//
// Metodo para RPC PedirVoto
//
func (nr *NodoRaft) PedirVoto(args *ArgsPeticionVoto, reply *RespuestaPeticionVoto) {
	nr.mux.Lock()
	if (nr.votedFor == -1 || nr.votedFor == args.CandidateId) && args.Term >= nr.currentTerm {
		nr.votedFor = args.CandidateId
		nr.currentTerm = args.Term
		nr.mux.Unlock()
		reply.Term = args.Term
		reply.VoteGranted = true
		if nr.estado == SEGUIDOR {
			nr.mensajeLatido <- true
		}
	} else {
		nr.mux.Unlock()
		reply.VoteGranted = false
		reply.Term = nr.currentTerm
	}
}

// Ejemplo de código enviarPeticionVoto
//
// nodo int -- indice del servidor destino en nr.nodos[]
//
// args *RequestVoteArgs -- argumentos par la llamada RPC
//
// reply *RequestVoteReply -- respuesta RPC
//
// Los tipos de argumentos y respuesta pasados a CallTimeout deben ser
// los mismos que los argumentos declarados en el metodo de tratamiento
// de la llamada (incluido si son punteros
//
// Si en la llamada RPC, la respuesta llega en un intervalo de tiempo,
// la funcion devuelve true, sino devuelve false
//
// la llamada RPC deberia tener un timout adecuado.
//
// Un resultado falso podria ser causado por una replica caida,
// un servidor vivo que no es alcanzable (por problemas de red ?),
// una petiión perdida, o una respuesta perdida
//
// Para problemas con funcionamiento de RPC, comprobar que la primera letra
// del nombre  todo los campos de la estructura (y sus subestructuras)
// pasadas como parametros en las llamadas RPC es una mayuscula,
// Y que la estructura de recuperacion de resultado sea un puntero a estructura
// y no la estructura misma.
//
func (nr *NodoRaft) enviarPeticionVoto(nodo int, args *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) bool {
	cliente, err := rpc.DialHTTP("tcp", nr.nodos[nodo])
	checkError(err)
	err = rpctimeout.CallTimeout(cliente, "NodoRaft.PedirVoto", &args, &reply, 25*time.Millisecond)
	return err == nil
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
	Term    int // mandato actual
	Success int // true si contiene una entrada que coincida con PrevLogIndex y PrevLogTerm
}
