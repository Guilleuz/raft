package main

import (
	"fmt"
	"net/rpc"
	"os"
	"raft/internal/raft"
	"runtime"
)

func checkError(err error) {
	if err != nil {
		_, _, linea, _ := runtime.Caller(1)
		fmt.Fprintf(os.Stderr, "Fatal error: %s\n", err.Error())
		fmt.Fprintf(os.Stderr, "Línea: %d\n", linea)
	}
}

func main() {
	var nodos []string
	// Los argumentos son los end points como strings
	for _, nodo := range os.Args[1:] {
		nodos = append(nodos, nodo)
	}

	for {
		// Pedimos operacion por pantalla
		fmt.Println("Seleccione una opción:\n1. Parar Nodo\n2. Estado Nodo\n3. Someter a Nodo")
		var operacion int
		fmt.Scan(&operacion)
		// Pedimos número de nodo por pantalla
		fmt.Print("Indique el nodo a aplicar la operación: ")
		var nodo int
		fmt.Scan(&nodo)

		// Comprobamos que el número de nodo sea correcto y que nos podemos conectar a él
		if nodo >= len(nodos) {
			continue
		}
		cliente, err := rpc.DialHTTP("tcp", nodos[nodo])
		checkError(err)
		if cliente == nil {
			continue
		}

		switch operacion {
		case 1:
			// Parar nodo
			err = cliente.Call("NodoRaft.ParaRPC", struct{}{}, struct{}{})
			checkError(err)
			fmt.Printf("Nodo %d detenido\n\n", nodo)
		case 2:
			// Conslutar estado del nodo
			var reply raft.ObtenerEstadoReply
			err = cliente.Call("NodoRaft.ObtenerEstadoRPC", struct{}{}, &reply)
			checkError(err)
			fmt.Printf("Estado del nodo %d: id:%d, mandato:%d, idLider:%d, esLider:%t\n\n",
				nodo, reply.Yo, reply.Mandato, reply.LiderId, reply.EsLider)
		case 3:
			// Someter operación al nodo
			var args interface{} = "Someto por RPC"
			var replyOP raft.SometerOperacionReply
			err = cliente.Call("NodoRaft.SometerOperacionRPC", &args, &replyOP)
			checkError(err)
			fmt.Printf("Resultados someter al nodo %d: indice:%d, mandato:%d, esLider:%t\n\n",
				nodo, replyOP.Indice, replyOP.Mandato, replyOP.EsLider)
		default:
			fmt.Println("Opción no reconocida\n")
		}
	}
}
