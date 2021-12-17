package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"raft/internal/raft"
	"runtime"
	"strconv"
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
	if err != nil {
		fmt.Printf("Main, mal numero entero de indice de nodo:")
		checkError(err)
		os.Exit(1)
	}

	var nodos []string
	// Resto de argumento son los end points como strings
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
	//rpc.HandleHTTP()
	fmt.Println("RPC OK")

	l, err := net.Listen("tcp", os.Args[2:][me])
	if err != nil {
		fmt.Printf("Main listen error:")
		checkError(err)
		os.Exit(1)
	}

	fmt.Println("Net Listen OK")

	fmt.Println("Replica escucha en :", me, " de ", os.Args[2:])
	// TODO cambiar conexiones http por tcp
	//http.Serve(l, nil)
	rpc.Accept(l)
}
