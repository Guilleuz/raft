package raft

import (
	"strconv"
	"time"
)

// Finaliza la ejecución de la réplica
func (nr *NodoRaft) Para() {
	nr.canalStop <- true
}

// Llamada RPC que finaliza la ejecución de la réplica
func (nr *NodoRaft) ParaRPC(_, _ *struct{}) error {
	nr.canalStop <- true
	return nil
}

// Devuelve "yo", mandato en curso, el id del lider y si este nodo cree ser lider
func (nr *NodoRaft) ObtenerEstado() (int, int, int, bool) {
	nr.logger.Printf("Réplica %d estado: mandato:%d, idLider:%d, estado:%d\n",
		nr.yo, nr.currentTerm, nr.lider, nr.estado)
	return nr.yo, nr.currentTerm, nr.lider, nr.estado == LIDER
}

type ObtenerEstadoReply struct {
	Yo      int
	Mandato int
	LiderId int
	EsLider bool
}

// Llamada RPC que implementa la funcionalidad Obtener estado
func (nr *NodoRaft) ObtenerEstadoRPC(_ *struct{}, reply *ObtenerEstadoReply) error {
	reply.Yo, reply.Mandato, reply.LiderId, reply.EsLider =
		nr.ObtenerEstado()
	return nil
}

type TipoOperacion struct {
	Operacion string // La operaciones posibles son "leer" y "escribir"
	Clave     string
	Valor     string // en el caso de la lectura Valor = ""
}

// Somete una operación, devolviendo el índice y
// el mandato en el que se introducirá si es comprometida
// Devolverá true si el nodo es líder, falso si no lo es
func (nr *NodoRaft) SometerOperacion(operacion TipoOperacion) (int, int, bool, int, string) {
	nr.mux.Lock()
	indice := len(nr.log)
	mandato := nr.currentTerm
	EsLider := nr.estado == LIDER
	idLider := nr.lider
	valor := ""
	nr.mux.Unlock()

	if EsLider {
		nr.mux.Lock()
		nr.log = append(nr.log, Operacion{nr.currentTerm, operacion})
		canalRespuesta := make(chan string, 100)
		nr.respuestas = append(nr.respuestas, CommitPendiente{canalRespuesta, indice})
		nr.mux.Unlock()
		go nr.AppendEntries(50 * time.Millisecond)
		nr.logger.Printf("Réplica %d: (lider) recibo una nueva operación, mandato: %d\n",
			nr.yo, nr.currentTerm)

		select {
		case valor = <-canalRespuesta:
		case <-time.After(1 * time.Second):
			nr.logger.Printf("Réplica %d: no he conseguido comprometer en un segundo, dejo de esperar\n",
				nr.yo)
		}
	}

	return indice, mandato, EsLider, idLider, valor
}

type SometerOperacionReply struct {
	Indice  int
	Mandato int
	EsLider bool
	IdLider int
	Valor   string
}

// Llamada RPC que implementa la funcionalidad SometerOperacion
func (nr *NodoRaft) SometerOperacionRPC(operacion TipoOperacion, reply *SometerOperacionReply) error {
	reply.Indice, reply.Mandato, reply.EsLider, reply.IdLider,
		reply.Valor = nr.SometerOperacion(operacion)
	return nil
}

// Devuelve una cadena con el log del nodo y su valor de commitIndex y lastApplied
// Log en formato indice:mandato indice+1:mandato ...
func (nr *NodoRaft) ObtenerEstadoLog() (string, int, int) {
	return nr.logToString(), nr.commitIndex, nr.lastApplied
}

type ObtenerEstadoLogReply struct {
	Log         string
	CommitIndex int
	LastApplied int
}

func (nr *NodoRaft) ObtenerEstadoLogRPC(_ *struct{}, reply *ObtenerEstadoLogReply) error {
	reply.Log, reply.CommitIndex, reply.LastApplied = nr.ObtenerEstadoLog()
	return nil
}

func (nr *NodoRaft) logToString() string {
	var cadenaLog string = ""
	nr.mux.Lock()
	for i, entrada := range nr.log {
		cadenaLog = cadenaLog + strconv.Itoa(i) + ":" + strconv.Itoa(entrada.Mandato) + " "
	}
	nr.mux.Unlock()
	return cadenaLog
}
