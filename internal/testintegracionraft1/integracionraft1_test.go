package testintegracionraft1

import (
	"fmt"
	"net/rpc"
	"os"
	"raft/internal/despliegue"
	"raft/internal/raft"
	"runtime"
	"strconv"
	"testing"
	"time"
)

const (
	//hosts
	MAQUINA_LOCAL = "127.0.0.1"
	MAQUINA1      = "155.210.154.194"
	MAQUINA2      = "155.210.154.195"
	MAQUINA3      = "155.210.154.196"

	//puertos
	PUERTOREPLICA1 = "17561"
	PUERTOREPLICA2 = "17562"
	PUERTOREPLICA3 = "17563"

	//nodos replicas
	REPLICA1 = MAQUINA1 + ":" + PUERTOREPLICA1
	REPLICA2 = MAQUINA2 + ":" + PUERTOREPLICA2
	REPLICA3 = MAQUINA3 + ":" + PUERTOREPLICA3

	// paquete main de ejecutables relativos a PATH previo
	EXECREPLICA = "./srvraft"

	// comandos completo a ejecutar en máquinas remota con ssh. Ejemplo :
	// 				cd $HOME/raft; go run cmd/srvraft/main.go 127.0.0.1:29001

	// go run testcltvts/main.go 127.0.0.1:29003 127.0.0.1:29001 127.0.0.1:29000
	REPLICACMD = "cd /home/a801618/3º/SSDD/raft/cmd/srvraft; " + EXECREPLICA

	// Ubicar, en esta constante, nombre de fichero de vuestra clave privada local
	// emparejada con la clave pública en authorized_keys de máquinas remotas
	PRIVKEYFILE = "id_rsa"

	// Si true, se escribirá por pantalla lo que escriban las aplicaciones
	// ejecutadas con SSH
	imprimirSSH = false
)

func checkError(err error) {
	if err != nil {
		_, _, linea, _ := runtime.Caller(1)
		fmt.Fprintf(os.Stderr, "Fatal error: %s\n", err.Error())
		fmt.Fprintf(os.Stderr, "Línea: %d\n", linea)
	}
}

// TEST primer rango
func TestPrimerasPruebas(t *testing.T) {
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	var cr CanalResultados
	cr.canal = make(chan string, 2000)
	cr.replicasMaquinas = map[string]string{REPLICA1: MAQUINA1,
		REPLICA2: MAQUINA2, REPLICA3: MAQUINA3}
	cr.replicas = []string{REPLICA1, REPLICA2, REPLICA3}

	if imprimirSSH {
		go func() {
			for {
				cadena := <-cr.canal
				fmt.Printf("CANAL-LOG:%s\n", cadena)
			}
		}()
	}

	// Test1 : Arranque y parada de un nodo
	t.Run("T1:ArranqueYParada",
		func(t *testing.T) { cr.soloArranqueYparadaTest1(t) })

	// Test2 : Elección de un primer líder
	t.Run("T2:ElegirPrimerLider",
		func(t *testing.T) { cr.ElegirPrimerLiderTest2(t) })

	// Test3: Elección de un líder tras el fallo del primero
	t.Run("T3:FalloAnteriorElegirNuevoLider",
		func(t *testing.T) { cr.FalloAnteriorElegirNuevoLiderTest3(t) })

	// Test4: réplicación correcta de 3 escrituras
	t.Run("T4:EscriturasConcurrentes",
		func(t *testing.T) { cr.tresOperacionesComprometidasEstable(t) })

}

// TEST primer rango
func TestAcuerdosConFallos(t *testing.T) { // (m *testing.M) {
	// <setup code>
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	var cr CanalResultados
	cr.canal = make(chan string, 2000)
	cr.replicasMaquinas = map[string]string{REPLICA1: MAQUINA1,
		REPLICA2: MAQUINA2, REPLICA3: MAQUINA3}
	cr.replicas = []string{REPLICA1, REPLICA2, REPLICA3}

	// Test5: Se consigue acuerdo a pesar de desconexiones de seguidor
	t.Run("T5:AcuerdoAPesarDeDesconexionesDeSeguidor ",
		func(t *testing.T) { cr.AcuerdoApesarDeSeguidor(t) })

	t.Run("T5:SinAcuerdoPorFallos ",
		func(t *testing.T) { cr.SinAcuerdoPorFallos(t) })

	t.Run("T5:SometerConcurrentementeOperaciones ",
		func(t *testing.T) { cr.SometerConcurrentementeOperaciones(t) })

}

// Se consigue acuerdo a pesar de desconexiones de seguidor -- 3 NODOS RAFT
func (cr *CanalResultados) AcuerdoApesarDeSeguidor(t *testing.T) {
	t.Skip("SKIPPED AcuerdoApesarDeSeguidor")

	fmt.Println(t.Name(), ".....................")
	cr.startDistributedProcesses(cr.replicasMaquinas)

	// Comprometer una entrada
	lider := cr.pruebaUnLider()
	if lider == -1 {
		cr.stopDistributedProcesses(cr.replicas)
		t.Errorf("No hay líder, o hay más de uno\n")
	}
	var args raft.TipoOperacion
	var replyOP raft.SometerOperacionReply
	args.Operacion = "escribir"
	args.Clave = "clave"
	args.Valor = "valor"

	cliente, err := rpc.Dial("tcp", cr.replicas[lider])
	err = cliente.Call("NodoRaft.SometerOperacionRPC", &args, &replyOP)
	checkError(err)
	if !replyOP.EsLider {
		cr.stopDistributedProcesses(cr.replicas)
		t.Errorf("Someto a un seguidor\n")
	}

	//  Obtener un lider y, a continuación desconectar uno de los nodos Raft
	cr.stopDistributedProcesses([]string{REPLICA1})
	// Esperamos por una posible eleccion
	lider = cr.pruebaUnLider()
	if lider == -1 {
		cr.stopDistributedProcesses(cr.replicas)
		t.Errorf("No hay líder, o hay más de uno\n")
	}
	cliente, err = rpc.Dial("tcp", cr.replicas[lider])
	checkError(err)

	// Comprobar varios acuerdos con una réplica desconectada
	for i := 0; i < 3; i++ {
		cliente, err := rpc.Dial("tcp", cr.replicas[lider])
		err = cliente.Call("NodoRaft.SometerOperacionRPC", &args, &replyOP)
		checkError(err)
		if !replyOP.EsLider {
			cr.stopDistributedProcesses(cr.replicas)
			t.Errorf("Someto a un seguidor\n")
		}
	}

	time.Sleep(150 * time.Millisecond)

	// Comprobamos que los acuerdos son correctos
	var replyLog raft.ObtenerEstadoLogReply
	err = cliente.Call("NodoRaft.ObtenerEstadoLogRPC", struct{}{}, &replyLog)
	checkError(err)
	fmt.Printf("Log del lider: %s\ncommitIndex: %d, lastApplied: %d\n",
		replyLog.Log, replyLog.CommitIndex, replyLog.LastApplied)
	if replyLog.CommitIndex != 4 {
		cr.stopDistributedProcesses(cr.replicas)
		t.Errorf("No se han sometido las 3 operaciones correctamente\n")
	}

	// reconectar nodo Raft previamente desconectado y comprobar varios acuerdos
	cr.startDistributedProcesses(map[string]string{REPLICA1: MAQUINA1})

	// Comprobar varios acuerdos con las 3 réplicas
	for i := 0; i < 3; i++ {
		cliente, err := rpc.Dial("tcp", cr.replicas[lider])
		err = cliente.Call("NodoRaft.SometerOperacionRPC", &args, &replyOP)
		checkError(err)
		if !replyOP.EsLider {
			cr.stopDistributedProcesses(cr.replicas)
			t.Errorf("Someto a un seguidor\n")
		}
	}

	time.Sleep(150 * time.Millisecond)

	err = cliente.Call("NodoRaft.ObtenerEstadoLogRPC", struct{}{}, &replyLog)
	checkError(err)
	fmt.Printf("Log del lider: %s\ncommitIndex: %d, lastApplied: %d\n",
		replyLog.Log, replyLog.CommitIndex, replyLog.LastApplied)
	if replyLog.CommitIndex != 7 {
		cr.stopDistributedProcesses(cr.replicas)
		t.Errorf("No se han sometido las 3 operaciones correctamente\n")
	}

	cr.stopDistributedProcesses(cr.replicas)
	fmt.Println(".............", t.Name(), "Superado")
}

// NO se consigue acuerdo al desconectarse mayoría de seguidores -- 3 NODOS RAFT
func (cr *CanalResultados) SinAcuerdoPorFallos(t *testing.T) {
	t.Skip("SKIPPED SinAcuerdoPorFallos")

	// Comprometer una entrada
	fmt.Println(t.Name(), ".....................")
	cr.startDistributedProcesses(cr.replicasMaquinas)

	// Comprometer una entrada
	lider := cr.pruebaUnLider()
	if lider == -1 {
		cr.stopDistributedProcesses(cr.replicas)
		t.Errorf("No hay líder, o hay más de uno\n")
	}
	var args raft.TipoOperacion
	var replyOP raft.SometerOperacionReply
	args.Operacion = "escribir"
	args.Clave = "clave"
	args.Valor = "valor"

	cliente, err := rpc.Dial("tcp", cr.replicas[lider])
	err = cliente.Call("NodoRaft.SometerOperacionRPC", &args, &replyOP)
	checkError(err)
	if !replyOP.EsLider {
		cr.stopDistributedProcesses(cr.replicas)
		t.Errorf("Someto a un seguidor\n")
	}

	// Obtener un lider y, a continuación desconectar 2 de los nodos Raft
	cr.stopDistributedProcesses([]string{cr.replicas[(lider+1)%3], cr.replicas[(lider+2)%3]})

	// Comprobar varios acuerdos con 2 réplicas desconectada
	for i := 0; i < 3; i++ {
		cliente, err := rpc.Dial("tcp", cr.replicas[lider])
		err = cliente.Call("NodoRaft.SometerOperacionRPC", &args, &replyOP)
		checkError(err)
		if !replyOP.EsLider {
			cr.stopDistributedProcesses(cr.replicas)
			t.Errorf("Someto a un seguidor\n")
		}
	}

	time.Sleep(150 * time.Millisecond)

	// Comprobamos que no se han realizado los acuerdos
	var replyLog raft.ObtenerEstadoLogReply
	err = cliente.Call("NodoRaft.ObtenerEstadoLogRPC", struct{}{}, &replyLog)
	checkError(err)
	fmt.Printf("Log del lider: %s\ncommitIndex: %d, lastApplied: %d\n",
		replyLog.Log, replyLog.CommitIndex, replyLog.LastApplied)
	if replyLog.CommitIndex != 1 {
		cr.stopDistributedProcesses(cr.replicas)
		t.Errorf("Ha sometido operaciones con dos nodos caídos\n")
	}

	// reconectar los 2 nodos Raft desconectados y probar varios acuerdos

	cr.startDistributedProcesses(cr.replicasMaquinas)
	time.Sleep(500 * time.Millisecond)
	cliente, err = rpc.Dial("tcp", cr.replicas[lider])
	err = cliente.Call("NodoRaft.SometerOperacionRPC", &args, &replyOP)
	checkError(err)
	if !replyOP.EsLider {
		cr.stopDistributedProcesses(cr.replicas)
		t.Errorf("Someto a un seguidor\n")
	}

	time.Sleep(150 * time.Millisecond)

	// Comprobamos que se han realizado los acuerdos pendientes
	err = cliente.Call("NodoRaft.ObtenerEstadoLogRPC", struct{}{}, &replyLog)
	checkError(err)
	fmt.Printf("Log del lider: %s\ncommitIndex: %d, lastApplied: %d\n",
		replyLog.Log, replyLog.CommitIndex, replyLog.LastApplied)
	if replyLog.CommitIndex != 5 {
		cr.stopDistributedProcesses(cr.replicas)
		t.Errorf("No ha sometido las operaciones pendientes\n")
	}

	cr.stopDistributedProcesses(cr.replicas)
	fmt.Println(".............", t.Name(), "Superado")
}

// Se somete 5 operaciones de forma concurrente -- 3 NODOS RAFT
func (cr *CanalResultados) SometerConcurrentementeOperaciones(t *testing.T) {
	//t.Skip("SKIPPED SometerConcurrentementeOperaciones")
	fmt.Println(t.Name(), ".....................")
	cr.startDistributedProcesses(cr.replicasMaquinas)

	// Obtener un lider y, a continuación someter 5 operaciones
	lider := cr.pruebaUnLider()
	if lider == -1 {
		cr.stopDistributedProcesses(cr.replicas)
		t.Errorf("No hay líder, o hay más de uno\n")
	}
	var args raft.TipoOperacion
	var replyOP raft.SometerOperacionReply
	args.Operacion = "escribir"
	args.Clave = "clave"
	args.Valor = "valor"

	for i := 0; i < 5; i++ {
		cliente, err := rpc.Dial("tcp", cr.replicas[lider])
		err = cliente.Call("NodoRaft.SometerOperacionRPC", &args, &replyOP)
		checkError(err)
		if !replyOP.EsLider {
			cr.stopDistributedProcesses(cr.replicas)
			t.Errorf("Someto a un seguidor\n")
		}
	}

	// Comprobar estados de nodos Raft, sobre todo
	// el avance del mandato en curso e indice de registro de cada uno
	// que debe ser identico entre ellos
	time.Sleep(150 * time.Millisecond)

	commitIndexEsperado := 5
	for i := 0; i < 3; i++ {
		cliente, err := rpc.Dial("tcp", cr.replicas[i])
		checkError(err)
		var replyLog raft.ObtenerEstadoLogReply
		err = cliente.Call("NodoRaft.ObtenerEstadoLogRPC", struct{}{}, &replyLog)
		checkError(err)
		fmt.Printf("Log del nodo %d: %s\ncommitIndex: %d, lastApplied: %d\n",
			i, replyLog.Log, replyLog.CommitIndex, replyLog.LastApplied)
		if replyLog.CommitIndex != commitIndexEsperado {
			t.Errorf("El nodo %d no ha comprometido las entradas correctamente\n", i)
		}
	}
	cr.stopDistributedProcesses(cr.replicas)
	fmt.Println(".............", t.Name(), "Superado")
}

// ---------------------------------------------------------------------
//
// Canal de resultados de ejecución de comandos ssh remotos
type CanalResultados struct {
	canal            chan string
	replicas         []string
	replicasMaquinas map[string]string
}

// start  gestor de vistas; mapa de replicas y maquinas donde ubicarlos;
// y lista clientes (host:puerto)
func (cr *CanalResultados) startDistributedProcesses(
	replicasMaquinas map[string]string) {
	listaReplicas := ""
	for replica := range replicasMaquinas {
		listaReplicas = listaReplicas + " " + replica
	}

	numeroReplica := 0
	for _, maquina := range replicasMaquinas {
		despliegue.ExecMutipleNodes(
			REPLICACMD+" "+strconv.Itoa(numeroReplica)+" "+listaReplicas,
			[]string{maquina}, cr.canal, PRIVKEYFILE)

		numeroReplica++
	}

	// dar tiempo para se establezcan las replicas
	time.Sleep(10000 * time.Millisecond)
}

// Finaliza la ejecución de las rélicas remotas
func (cr *CanalResultados) stopDistributedProcesses(replicas []string) {

	// Parar procesos que han sido ejecutados con rpc
	for _, replica := range replicas {
		fmt.Println("Replica a parar: " + replica)
		cliente, err := rpc.Dial("tcp", replica)
		checkError(err)
		if cliente != nil {
			err = cliente.Call("NodoRaft.ParaRPC", struct{}{}, struct{}{})
			checkError(err)
		}
	}
}

// --------------------------------------------------------------------------
// FUNCIONES DE SUBTESTS

// Se pone en marcha una replica
func (cr *CanalResultados) soloArranqueYparadaTest1(t *testing.T) {
	t.Skip("SKIPPED soloArranqueYparadaTest1")

	fmt.Println(t.Name(), ".....................")

	// Poner en marcha replicas en remoto
	cr.startDistributedProcesses(map[string]string{REPLICA1: MAQUINA1})

	// Parar réplicas alamcenamiento en remoto
	cr.stopDistributedProcesses([]string{REPLICA1})

	fmt.Println(".............", t.Name(), "Superado")
}

// Primer lider en marcha
func (cr *CanalResultados) ElegirPrimerLiderTest2(t *testing.T) {
	t.Skip("SKIPPED ElegirPrimerLiderTest2")

	fmt.Println(t.Name(), ".....................")

	// Poner en marcha  3 réplicas Raft
	cr.startDistributedProcesses(cr.replicasMaquinas)

	// Se ha elegido lider ?
	fmt.Printf("Probando lider en curso\n")
	if cr.pruebaUnLider() == -1 {
		t.Errorf("No hay líder, o hay más de uno\n")
	}

	cr.stopDistributedProcesses(cr.replicas)

	fmt.Println(".............", t.Name(), "Superado")
}

// Fallo de un primer lider y reeleccion de uno nuevo
func (cr *CanalResultados) FalloAnteriorElegirNuevoLiderTest3(t *testing.T) {
	t.Skip("SKIPPED FalloAnteriorElegirNuevoLiderTest3")

	fmt.Println(t.Name(), ".....................")

	// Poner en marcha  3 réplicas Raft
	cr.startDistributedProcesses(cr.replicasMaquinas)

	fmt.Printf("Lider inicial\n")
	lider := cr.pruebaUnLider()

	// Desconectar lider
	if lider != -1 {
		cr.stopDistributedProcesses([]string{cr.replicas[lider]})
	} else {
		t.Errorf("No hay líder, o hay más de uno\n")
	}

	fmt.Printf("Comprobar nuevo lider\n")
	if cr.pruebaUnLider() == -1 {
		t.Errorf("No hay un nuevo líder, o hay más de uno\n")
	}

	// Parar réplicas almacenamiento en remoto
	cr.stopDistributedProcesses(cr.replicas)

	fmt.Println(".............", t.Name(), "Superado")
}

// 3 operaciones comprometidas con situacion estable y sin fallos
func (cr *CanalResultados) tresOperacionesComprometidasEstable(t *testing.T) {
	//t.Skip("SKIPPED tresOperacionesComprometidasEstable")
	fmt.Println(t.Name(), ".....................")

	// Poner en marcha  3 réplicas Raft
	cr.startDistributedProcesses(cr.replicasMaquinas)

	lider := cr.pruebaUnLider()
	if lider != -1 {
		cliente, err := rpc.Dial("tcp", cr.replicas[lider])
		checkError(err)
		// SI cliente nil no superado
		if cliente == nil {
			t.Errorf("No se ha podido conectar con el lider\n")
		}

		var args interface{} = "Someto por RPC"
		var replyOP raft.SometerOperacionReply
		err = cliente.Call("NodoRaft.SometerOperacionRPC", &args, &replyOP)
		checkError(err)
		fmt.Printf("Resultados someter 1: %d, %d, %t\n",
			replyOP.Indice, replyOP.Mandato, replyOP.EsLider)

		err = cliente.Call("NodoRaft.SometerOperacionRPC", &args, &replyOP)
		checkError(err)
		fmt.Printf("Resultados someter 2: %d, %d, %t\n",
			replyOP.Indice, replyOP.Mandato, replyOP.EsLider)

		err = cliente.Call("NodoRaft.SometerOperacionRPC", &args, &replyOP)
		checkError(err)
		fmt.Printf("Resultados someter 3: %d, %d, %t\n",
			replyOP.Indice, replyOP.Mandato, replyOP.EsLider)

		if replyOP.Indice != 3 {
			t.Errorf("No se han registrado las entradas correctamente\n")
		}
	} else {
		t.Errorf("No hay líder, o hay más de uno\n")
	}

	// Parar réplicas almacenamiento en remoto
	cr.stopDistributedProcesses(cr.replicas)
	fmt.Println(".............", t.Name(), "Superado")
}

// --------------------------------------------------------------------------
// FUNCIONES DE APOYO
// Comprobar que hay un solo lider
func (cr *CanalResultados) pruebaUnLider() int {
	lider := -1
	for _, replica := range cr.replicas {
		cliente, err := rpc.Dial("tcp", replica)
		checkError(err)
		reply := new(raft.ObtenerEstadoReply)
		if cliente != nil {
			err = cliente.Call("NodoRaft.ObtenerEstadoRPC", struct{}{}, reply)
			checkError(err)
			fmt.Printf("Estado Réplica %d: mandato:%d, idLider:%d, esLider:%t\n",
				reply.Yo, reply.Mandato, reply.LiderId, reply.EsLider)
			if reply.EsLider && lider == -1 {
				lider = reply.Yo
			} else if reply.EsLider {
				return -1 // Hay más de un líder
			}
		}
	}

	return lider
}
