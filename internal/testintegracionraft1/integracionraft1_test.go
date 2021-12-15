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

	// PATH de los ejecutables de modulo golang de servicio de vistas
	//PATH = filepath.Join(os.Getenv("HOME"), "tmp", "P4", "raft")

	// paquete main de ejecutables relativos a PATH previo
	EXECREPLICA = "./srvraft"

	// comandos completo a ejecutar en máquinas remota con ssh. Ejemplo :
	// 				cd $HOME/raft; go run cmd/srvraft/main.go 127.0.0.1:29001

	// go run testcltvts/main.go 127.0.0.1:29003 127.0.0.1:29001 127.0.0.1:29000
	REPLICACMD = "cd /home/a801618/3º/SSDD/raft/cmd/srvraft; " + EXECREPLICA

	// Ubicar, en esta constante, nombre de fichero de vuestra clave privada local
	// emparejada con la clave pública en authorized_keys de máquinas remotas

	PRIVKEYFILE = "id_rsa"
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
	cr.replicasMaquinas = map[string]string{REPLICA1: MAQUINA1, REPLICA2: MAQUINA2, REPLICA3: MAQUINA3}
	cr.replicas = []string{REPLICA1, REPLICA2, REPLICA3}

	go func() {
		for {
			cadena := <- cr.canal
			fmt.Printf("CANAL-LOG:%s\n", cadena)
		}
	}()

	// Test1 : No debería haber ningun primario, si SV no ha recibido aún latidos
	t.Run("T1:ArranqueYParada",
		func(t *testing.T) { cr.soloArranqueYparadaTest1(t) })

	// Test2 : No debería haber ningun primario, si SV no ha recibido aún latidos
	t.Run("T2:ElegirPrimerLider",
		func(t *testing.T) { cr.ElegirPrimerLiderTest2(t) })

	// Test3: tenemos el primer primario correcto
	t.Run("T3:FalloAnteriorElegirNuevoLider",
		func(t *testing.T) { cr.FalloAnteriorElegirNuevoLiderTest3(t) })

	// Test4: Primer nodo copia
	t.Run("T4:EscriturasConcurrentes",
		func(t *testing.T) { cr.tresOperacionesComprometidasEstable(t) })
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
	for replica, _ := range replicasMaquinas {
		listaReplicas = listaReplicas + " " + replica
	}

	numeroReplica := 0
	for _, maquina := range replicasMaquinas {
		despliegue.ExecMutipleNodes(
			REPLICACMD+" "+strconv.Itoa(numeroReplica)+" "+listaReplicas,
			[]string{maquina}, cr.canal, PRIVKEYFILE)

		// dar tiempo para se establezcan las replicas
		time.Sleep(10000 * time.Millisecond)
		numeroReplica++
	}
}

//
func (cr *CanalResultados) stopDistributedProcesses(replicas []string) {

	// Parar procesos que han sido distribuidos con rpc
	for _, replica := range replicas {
		fmt.Println("Replica a parar: " + replica)
		cliente, err := rpc.DialHTTP("tcp", replica)
		checkError(err)
		if cliente != nil {
			err = cliente.Call("NodoRaft.ParaRPC", struct{}{}, struct{}{})
			checkError(err)
		}
	}
}

// --------------------------------------------------------------------------
// FUNCIONES DE SUBTESTS

// Se pone en marcha una replica ??
func (cr *CanalResultados) soloArranqueYparadaTest1(t *testing.T) {
	//t.Skip("SKIPPED soloArranqueYparadaTest1")

	fmt.Println(t.Name(), ".....................")

	// Poner en marcha replicas en remoto
	cr.startDistributedProcesses(map[string]string{REPLICA1: MAQUINA1})

	// Parar réplicas alamcenamiento en remoto
	cr.stopDistributedProcesses([]string{REPLICA1})

	fmt.Println(".............", t.Name(), "Superado")
}

// Primer lider en marcha
func (cr *CanalResultados) ElegirPrimerLiderTest2(t *testing.T) {
	//t.Skip("SKIPPED ElegirPrimerLiderTest2")

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
	//t.Skip("SKIPPED FalloAnteriorElegirNuevoLiderTest3")

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
	fmt.Println(t.Name(), ".....................")

	// Poner en marcha  3 réplicas Raft
	cr.startDistributedProcesses(cr.replicasMaquinas)

	lider := cr.pruebaUnLider()
	if lider != -1 {
		cliente, err := rpc.DialHTTP("tcp", cr.replicas[lider])
		checkError(err)
		// SI cliente nil no superado
		if cliente == nil {
			t.Errorf("No se ha podido conectar con el lider\n")
		}

		var args interface{} = "Someto por RPC"
		var replyOP raft.SometerOperacionReply
		err = cliente.Call("NodoRaft.SometerOperacionRPC", &args, &replyOP)
		checkError(err)
		fmt.Printf("Resultados someter 1: %d, %d, %t\n", replyOP.Indice, replyOP.Mandato, replyOP.EsLider)

		err = cliente.Call("NodoRaft.SometerOperacionRPC", &args, &replyOP)
		checkError(err)
		fmt.Printf("Resultados someter 2: %d, %d, %t\n", replyOP.Indice, replyOP.Mandato, replyOP.EsLider)

		err = cliente.Call("NodoRaft.SometerOperacionRPC", &args, &replyOP)
		checkError(err)
		fmt.Printf("Resultados someter 3: %d, %d, %t\n", replyOP.Indice, replyOP.Mandato, replyOP.EsLider)
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
// probar varias veces si se necesitan reelecciones
func (cr *CanalResultados) pruebaUnLider() int {
	lider := -1
	for _, replica := range cr.replicas {
		cliente, err := rpc.DialHTTP("tcp", replica)
		checkError(err)
		reply := new(raft.ObtenerEstadoReply)
		if cliente != nil {
			err = cliente.Call("NodoRaft.ObtenerEstadoRPC", struct{}{}, reply)
			checkError(err)
			fmt.Printf("Estado Réplica %d: mandato:%d, idLider:%d, esLider:%t\n", reply.Yo, reply.Mandato, reply.LiderId, reply.EsLider)
			if reply.EsLider && lider == -1 {
				lider = reply.Yo
			} else if reply.EsLider {
				return -1 // Hay más de un líder
			}	
		}
	}

	return lider
}
