// Escribir vuestro código de funcionalidad Raft en este fichero
//

package raft

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"
)

//  false deshabilita por completo los logs de depuracion
// Aseguraros de poner kEnableDebugLogs a false antes de la entrega
const kEnableDebugLogs = true

// Poner a true para logear a stdout en lugar de a fichero
const kLogToStdout = false

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
	canalRespuesta chan string
	indice         int // en la entrada de registro
	operacion      TipoOperacion
}

type Operacion struct {
	Mandato   int
	Operacion TipoOperacion
}

type CommitPendiente struct {
	canalRespuesta chan string
	indice         int
}

// Tipo de dato Go que representa un solo nodo (réplica) de raft
type NodoRaft struct {
	mux sync.Mutex // Mutex para proteger acceso a estado compartido

	nodos []string // IP:Puerto de todas las réplicas
	yo    int      // this peer's index into peers[]
	lider int      // id de la replica que cree que es lider
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

	canalAplicar  chan AplicaOperacion
	mensajeLatido chan bool
	canalStop     chan bool

	respuestas []CommitPendiente
}

func NuevoNodo(nodos []string, yo int, canalAplicar chan AplicaOperacion) *NodoRaft {
	// Inicializamos el nodo
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
	// Inicializamos el log de forma que todas las réplicas tengan una entrada inicial igual
	nr.log = []Operacion{{0, TipoOperacion{}}}

	nr.estado = SEGUIDOR
	nr.nextIndex = make([]int, len(nodos))
	nr.matchIndex = make([]int, len(nodos))
	nr.lider = -1

	nr.commitIndex = 0
	nr.lastApplied = 0
	nr.canalAplicar = make(chan AplicaOperacion, 100)

	if kEnableDebugLogs {
		nombreNodo := nodos[yo]
		logPrefix := fmt.Sprintf("%s", nombreNodo)
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
	go maquinaDeEstados(nr.canalAplicar, nr.logger)
	return nr
}

// Función encargada de la gestión del estado de la réplica
func (nr *NodoRaft) gestionEstado() {
	nr.logger.Printf("Réplica %d: comienza la ejecución\n", nr.yo)
	rand.Seed(time.Now().UnixNano())
	for {
		select {
		case <-nr.canalStop:
			nr.logger.Printf("Réplica %d: finaliza la ejecución\n", nr.yo)
			os.Exit(0)
		default:
			nr.mux.Lock()
			estadoActual := nr.estado
			nr.mux.Unlock()
			switch estadoActual {
			case SEGUIDOR:
				timeout := time.After(time.Duration(rand.Intn(151)+150) * time.Millisecond)
				select {
				case <-nr.mensajeLatido:
					// Si recibimos un mensaje, se reinicia el timeout
				case <-timeout:
					// Timeout aleatorio entre 150 y 300ms
					// Si expira, pasamos a candidato
					nr.logger.Printf("Réplica %d: vence el timeout, paso a candidato\n", nr.yo)
					nr.mux.Lock()
					nr.votedFor = nr.yo
					nr.estado = CANDIDATO
					nr.mux.Unlock()
				}
			case CANDIDATO:
				// Comenzamos una elección
				nr.logger.Printf("Réplica %d: antes del inicio de la eleccion\n", nr.yo)
				nr.eleccion()
			case LIDER:
				// latido 20 veces por segundo (cada 50 ms)
				nr.logger.Printf("Réplica %d: envío latido, mandato: %d\n",
					nr.yo, nr.currentTerm)
				nr.AppendEntries(50 * time.Millisecond)
			}
		}
	}
}

// Función encargada de la gestión de la máquina de estados
// Permite operaciones de lectura y escritura en una tabla hash
func maquinaDeEstados(canalAplicar chan AplicaOperacion, logger *log.Logger) {
	almacenamiento := make(map[string]string)
	for {
		aplica := <-canalAplicar
		if aplica.operacion.Operacion == "escribir" {
			almacenamiento[aplica.operacion.Clave] = aplica.operacion.Valor
			aplica.canalRespuesta <- ""
			logger.Printf("Aplicada la operación escribir %s:%s\n",
				aplica.operacion.Clave, aplica.operacion.Valor)
		} else if aplica.operacion.Operacion == "leer" {
			aplica.canalRespuesta <- almacenamiento[aplica.operacion.Clave]
			logger.Printf("Aplicada la operación leer %s:%s\n", aplica.operacion.Clave)
		} else {
			logger.Println("Operacion no reconocida")
		}
	}
}
