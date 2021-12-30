package raft

import "time"

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
	nr.mux.Lock()
	reply.Yo = nr.yo
	reply.Mandato = nr.currentTerm
	reply.LiderId = nr.lider
	reply.EsLider = nr.estado == LIDER
	nr.logger.Printf("Réplica %d estado: mandato:%d, idLider:%d, estado:%d\n",
		nr.yo, nr.currentTerm, nr.lider, nr.estado)
	nr.mux.Unlock()
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
		canalRespuesta := make(chan string)
		nr.respuestas = append(nr.respuestas, CommitPendiente{canalRespuesta, indice})
		nr.mux.Unlock()
		go nr.AppendEntries(50 * time.Millisecond)
		nr.logger.Printf("Réplica %d: (lider) recibo una nueva operación, mandato: %d\n",
			nr.yo, nr.currentTerm)
		valor = <-canalRespuesta
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
	nr.mux.Lock()
	reply.Indice = len(nr.log)
	reply.Mandato = nr.currentTerm
	reply.EsLider = nr.estado == LIDER
	reply.Valor = ""
	nr.mux.Unlock()

	if reply.EsLider {
		nr.mux.Lock()
		nr.log = append(nr.log, Operacion{nr.currentTerm, operacion})
		canalRespuesta := make(chan string)
		nr.respuestas = append(nr.respuestas, CommitPendiente{canalRespuesta, reply.Indice})
		nr.mux.Unlock()
		go nr.AppendEntries(50 * time.Millisecond)
		nr.logger.Printf("Réplica %d: (lider) recibo una nueva operación, mandato: %d\n",
			nr.yo, nr.currentTerm)
		reply.Valor = <-canalRespuesta
	}

	return nil
}
