package raft

import "time"

// Metodo Para() utilizado cuando no se necesita mas al nodo
//
// Quizas interesante desactivar la salida de depuracion
// de este nodo
//
func (nr *NodoRaft) Para() {
	nr.canalStop <- true
}

func (nr *NodoRaft) ParaRPC(_, _ *struct{}) error {
	nr.canalStop <- true
	return nil
}

// Devuelve "yo", mandato en curso, el id del lider y si este nodo cree ser lider
func (nr *NodoRaft) ObtenerEstado() (int, int, int, bool) {
	return nr.yo, nr.currentTerm, nr.lider, nr.estado == LIDER
}

type ObtenerEstadoReply struct {
	Yo int
	Mandato int
	LiderId int
	EsLider bool
}
func (nr *NodoRaft) ObtenerEstadoRPC(_ *struct{}, reply *ObtenerEstadoReply) error {
	reply.Yo = nr.yo
	reply.Mandato = nr.currentTerm
	reply.LiderId = nr.lider
	reply.EsLider = nr.estado == LIDER 
	return nil
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

type SometerOperacionReply struct {
	Indice int
	Mandato int
	EsLider bool
}

func (nr *NodoRaft) SometerOperacionRPC(operacion interface{}, reply *SometerOperacionReply) error {
	nr.mux.Lock()
	nr.logger.Printf("Réplica %d: someter operacion: %d, %d, %d\n", nr.yo, len(nr.log), nr.currentTerm, nr.estado)
	reply.Indice = len(nr.log)
	reply.Mandato = nr.currentTerm
	reply.EsLider = nr.estado == LIDER
	nr.mux.Unlock()

	if reply.EsLider {
		nr.mux.Lock()
		nr.log = append(nr.log, Operacion{nr.currentTerm, operacion})
		go nr.AppendEntries([]Operacion{{nr.currentTerm, operacion}}, 50*time.Millisecond)
		nr.mux.Unlock()
		nr.logger.Printf("Réplica %d: (lider) recibo una nueva operación, mandato: %d\n", nr.yo, nr.currentTerm)
	
	}

	return nil
}
