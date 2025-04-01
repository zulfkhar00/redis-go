package commands

import (
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/db"
)

type ServerController interface {
	AddReplica(conn net.Conn)
	IsServerWaiting() bool
	SetServerIsWaiting(newStatus bool)
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
