package commands

import (
	"fmt"
	"strconv"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

// WaitCommand handles Redis WAIT command
type WaitCommand struct{}

func (c *WaitCommand) Name() string {
	return "replwaitconf"
}

func (c *WaitCommand) Execute(ctx *CommandContext) error {
	if len(ctx.Args) != 3 {
		return fmt.Errorf("supposed to get WAIT <replica_count> <timeout>, but got: %v", ctx.Args)
	}
	replicaCount, err := strconv.Atoi(ctx.Args[1]) // replicaCount
	if err != nil {
		return fmt.Errorf("WAIT <replica_count> should be number, but got: %s", ctx.Args[1])
	}
	timeout := ctx.Args[2]
	timeoutMs, err := strconv.Atoi(timeout)
	if err != nil {
		return fmt.Errorf("WAIT <timeout> should be number, but got: %s", ctx.Args[2])
	}

	if ctx.Store.Length() == 0 {
		_, err = ctx.Connection.Write([]byte(protocol.FormatRESPInt(int64(len(ctx.ReplicaConnections)))))
		if err != nil {
			return fmt.Errorf("error writing to connection: %v", err)
		}
		return nil
	}

	ctx.ServerControl.SetServerIsWaiting(true)
	// err = handleReplconfCommand([]string{"REPLCONF", "GETACK", "*"}, server, connection)
	for _, replica := range ctx.ReplicaConnections {
		go func() {
			_, err = replica.Write([]byte(protocol.FormatRESPArray([]string{"REPLCONF", "GETACK", "*"})))
			if err != nil {
				fmt.Printf("[master->replica] error writing to connection: %v\n", err)
			}
		}()
	}
	if err != nil {
		fmt.Printf("[handleWaitCommand] handleReplconfCommand error: %v\n", err)
	}
	timer := time.After(time.Duration(timeoutMs) * time.Millisecond)
	ackReceived := 0
	done := false
	for !done {
		select {
		case <-ctx.AckChan:
			ackReceived++
			fmt.Printf("ack received: %d\n", ackReceived)
			if ackReceived >= replicaCount {
				done = true
			}
		case <-timer:
			fmt.Printf("timeout\n")
			done = true
		}
	}
	ctx.ServerControl.SetServerIsWaiting(false)
	_, err = ctx.Connection.Write([]byte(protocol.FormatRESPInt(int64(ackReceived))))
	if err != nil {
		return fmt.Errorf("error writing to connection: %v", err)
	}

	return nil
}
