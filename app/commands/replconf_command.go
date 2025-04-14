package commands

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

// ReplconfCommand handles Redis REPLCONF command
type ReplconfCommand struct{}

func (c *ReplconfCommand) Name() string {
	return "replconf"
}

func (c *ReplconfCommand) Execute(ctx *CommandContext) error {
	var buf []byte
	receiveReplicaConfig(ctx.Args)
	fmt.Printf("Global master received: %v\n", ctx.Args)
	if strings.ToLower(ctx.Args[1]) == "getack" {
		buf = []byte(protocol.FormatRESPBulkStringsArray([]string{"REPLCONF", "GETACK", "*"}))
		if len(ctx.ReplicaConnections) == 0 {
			return fmt.Errorf("no replicas found")
		}
		// TODO: extend for all replicas
		replica := ctx.ReplicaConnections[0]
		_, err := replica.Write(buf)
		fmt.Printf("[inside] Master sent to replica: %v\n", []string{"REPLCONF", "GETACK", "*"})
		if err != nil {
			fmt.Printf("[inside] 1\n")
			return fmt.Errorf("cannot write to connection for GETACK: %v", err)
		}
		return nil
	}
	if strings.ToLower(ctx.Args[1]) == "ack" {
		if len(ctx.Args) != 3 {
			return fmt.Errorf("didn't get offset for REPLCONF ACK <offset>")
		}
		_, err := strconv.Atoi(ctx.Args[2])
		if err != nil {
			return fmt.Errorf("REPLCONF ACK <offset>: offset is not a number")
		}
		if ctx.ServerControl.IsServerWaiting() {
			ctx.AckChan <- true
		}
		return nil
	}

	fmt.Printf("Outside of replconf getack\n")
	buf = protocol.AppendSimpleString(buf, "OK")
	fmt.Printf("Master sent: OK\n")
	_, err := ctx.Connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}

	return nil
}

func (c *ReplconfCommand) DryExecute(ctx *CommandContext) (string, error) {
	return "", nil
}

func receiveReplicaConfig(cmd []string) {
	if len(cmd) != 3 {
		return
	}
	if strings.ToLower(cmd[1]) == "listening-port" {
		// replicaPort
		_ = cmd[2]
		return
	}
	if strings.ToLower(cmd[1]) == "capa" && strings.ToLower(cmd[2]) == "psync2" {
		// last phase of PART 2 of handshake
		return
	}
}
