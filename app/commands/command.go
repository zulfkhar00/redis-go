package commands

import (
	"fmt"
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/db"
	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

type ServerController interface {
	AddReplica(conn net.Conn)
	IsServerWaiting() bool
	SetServerIsWaiting(newStatus bool)
	TurnMultiOn(clientAdr string)
}

type RedisCommand interface {
	Execute(ctx *CommandContext) error
	Name() string
}

type CommandContext struct {
	Args       []string
	Store      *db.Store
	Connection net.Conn
	Config     *config.Config
	Role       ServerRole

	ReplicaConnections []net.Conn
	AckChan            chan bool
	ServerControl      ServerController
}

type ServerRole int

const (
	Master ServerRole = iota
	Slave
)

func (r ServerRole) String() string {
	switch r {
	case Master:
		return "master"
	case Slave:
		return "slave"
	default:
		return "unknown"
	}
}

func handleError(conn net.Conn, err error) error {
	_, writeErr := conn.Write([]byte(protocol.FormatRESPError(err)))
	if writeErr != nil {
		return fmt.Errorf("error writing to connection: %v", writeErr)
	}
	return nil
}

func writeResponse(conn net.Conn, res string) error {
	_, err := conn.Write([]byte(res))
	if err != nil {
		return fmt.Errorf("error writing to connection: %v", err)
	}
	return nil
}
