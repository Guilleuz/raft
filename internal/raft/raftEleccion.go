package raft

import (
	"math/rand"
	"net/rpc"
	"raft/internal/comun/rpctimeout"
	"time"
)

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
				nr.lider = nr.yo
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
