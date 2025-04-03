package server

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/commands"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/db"
	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

type Server struct {
	cfg             *config.Config
	kvStore         *db.Store
	commandRegistry *commands.CommandRegistry
}

var replicaConnections []net.Conn
var ackChan = make(chan bool)
var isWaiting = false
var connToMultiFlag = make(map[string]bool)
var connToQueuedCmds = make(map[string][]string)

func NewServer(cfg *config.Config, kvStore *db.Store) *Server {
	server := &Server{
		cfg,
		kvStore,
		commands.NewCommandRegistry(),
	}
	server.RegisterCommands()
	return server
}

func (server *Server) RegisterCommands() {
	server.commandRegistry.Register("command")
	server.commandRegistry.Register("ping")
	server.commandRegistry.Register("echo")
	server.commandRegistry.Register("set")
	server.commandRegistry.Register("get")
	server.commandRegistry.Register("config")
	server.commandRegistry.Register("keys")
	server.commandRegistry.Register("info")
	server.commandRegistry.Register("replconf")
	server.commandRegistry.Register("psync")
	server.commandRegistry.Register("wait")
	server.commandRegistry.Register("type")
	server.commandRegistry.Register("xadd")
	server.commandRegistry.Register("xrange")
	server.commandRegistry.Register("xread")
	server.commandRegistry.Register("incr")
	server.commandRegistry.Register("multi")
}

func (server *Server) Start() error {
	// server logic
	l, err := net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(server.cfg.Port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	for {
		connection, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			connection.Close()
			return err
		}

		go server.handleConnection(connection)
	}
}

func (server *Server) handleConnection(connection net.Conn) (err error) {
	defer connection.Close()

	reader := bufio.NewReader(connection)
	for {
		cmd, _, err := protocol.ReadRedisCommand(reader)
		if err != nil {
			return errors.New("parse command")
		}
		fmt.Printf("[master] received cmd: %v\n", cmd)

		err = handleCommand(cmd, server, connection)
		if err != nil {
			fmt.Printf("%v\n", err)
		}
	}
}

func handleCommand(cmd []string, server *Server, connection net.Conn) error {
	ctx := &commands.CommandContext{
		Args:               cmd,
		Store:              server.kvStore,
		Connection:         connection,
		Config:             server.cfg,
		Role:               commands.Master,
		ReplicaConnections: replicaConnections,
		AckChan:            ackChan,
		ServerControl:      server,
	}

	command := server.commandRegistry.Get(cmd[0])

	return command.Execute(ctx)
}

func (server *Server) AddReplica(conn net.Conn) {
	replicaConnections = append(replicaConnections, conn)
}

func (server *Server) IsServerWaiting() bool {
	return isWaiting
}

func (server *Server) SetServerIsWaiting(newStatus bool) {
	isWaiting = newStatus
}

func (server *Server) TurnMultiOn(clientAdr string) {
	connToMultiFlag[clientAdr] = true
}
