package main

import (
	//"errors"
	//"fmt"
	//"log"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"raft/internal/comun/check"
	"raft/internal/raft"
	"runtime"
	"strconv"
	//"time"
)

func checkError(err error) {
	if err != nil {
		_, _, linea, _ := runtime.Caller(1)
		fmt.Fprintf(os.Stderr, "Fatal error: %s\n", err.Error())
		fmt.Fprintf(os.Stderr, "Línea: %d\n", linea)
	}
}

func main() {
	// obtener entero de indice de este nodo
	me, err := strconv.Atoi(os.Args[1])
	check.CheckError(err, "Main, mal numero entero de indice de nodo:")

	var nodos []string
	// Resto de argumento son los end points como strings
	// De todas la replicas-> pasarlos a HostPort
	for _, nodo := range os.Args[2:] {
		nodos = append(nodos, nodo)
	}

	for i, nodo := range nodos {
		fmt.Printf("Nodo %d: %s\n", i, nodo)
	}

	// Parte Servidor
	nr := raft.NuevoNodo(nodos, me, make(chan raft.AplicaOperacion, 1000))
	err = rpc.Register(nr)
	checkError(err)
	rpc.HandleHTTP()

	fmt.Println("Replica escucha en :", me, " de ", os.Args[2:])

	l, err := net.Listen("tcp", os.Args[2:][me])
	check.CheckError(err, "Main listen error:")

	/*go func() {
		// Probar 3 appendEntries
		time.Sleep(500 * time.Millisecond)
		_, _, _, esLider := nr.ObtenerEstado()
		if esLider {
			fmt.Printf("Replica %d: es el lider", me)
			cliente, err := rpc.DialHTTP("tcp", os.Args[2])
			checkError(err)

			var reply raft.ObtenerEstadoReply
			err = cliente.Call("NodoRaft.ObtenerEstadoRPC", struct{}{}, &reply)
			checkError(err)
			fmt.Printf("Estado: %d, %d, %d, %d", reply.Yo, reply.Mandato, reply.LiderId, reply.EsLider)

			lider, err := rpc.DialHTTP("tcp", os.Args[2 + reply.LiderId])
			checkError(err)

			var args interface{} = "Someto por RPC"
			var replyOP raft.SometerOperacionReply
			err = lider.Call("NodoRaft.SometerOperacionRPC", &args, &replyOP)
			checkError(err)
			fmt.Printf("Resultados someter: %d, %d, %d\n", replyOP.Indice, replyOP.Mandato, replyOP.EsLider)

			err = cliente.Call("NodoRaft.ParaRPC", struct{}{}, struct{}{})
			checkError(err)
		}
	}()*/
	http.Serve(l, nil)
}
