package commands

import (
	"fmt"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

// SetCommand handles Redis SET command
type SetCommand struct{}

func (c *SetCommand) Name() string {
	return "set"
}

func (c *SetCommand) Execute(ctx *CommandContext) error {
	var buf []byte
	var result string
	key, val := ctx.Args[1], ctx.Args[2]
	if len(ctx.Args) == 3 {
		result = ctx.Store.Set(key, val, -1)
	} else if len(ctx.Args) == 5 && ctx.Args[3] == "px" {
		expire := ctx.Args[4]
		expireTimeMs, err := strconv.Atoi(expire)
		if err != nil {
			return fmt.Errorf("expire time is not a number: %v", err)
		}
		result = ctx.Store.Set(key, val, expireTimeMs)
	}
	buf = protocol.AppendSimpleString(buf, result)
	_, err := ctx.Connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}

	// replicate command to replicas
	for _, replica := range ctx.ReplicaConnections {
		_, err = replica.Write([]byte(protocol.FormatRESPArray(ctx.Args)))
		if err != nil {
			fmt.Printf("couldn't propogate `set` command to replica")
		}
	}

	return nil
}
