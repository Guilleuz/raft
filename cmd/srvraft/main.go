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

	http.Serve(l, nil)
}
