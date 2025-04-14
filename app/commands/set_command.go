package commands

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

// SetCommand handles Redis SET command
type SetCommand struct{}

func (c *SetCommand) Name() string {
	return "set"
}

func (c *SetCommand) Execute(ctx *CommandContext) error {
	clientAdr := ctx.Connection.RemoteAddr().String()
	if ctx.ServerControl.IsTransactionStarted(clientAdr) {
		ctx.ServerControl.AddTransactionCommand(clientAdr, ctx)
		_, err := ctx.Connection.Write([]byte("+QUEUED\r\n"))
		if err != nil {
			return fmt.Errorf("cannot write to connection: %v", err)
		}
		return nil
	}
	ctx.WaitForCommandFinish = true
	result, err := c.DryExecute(ctx)
	if err != nil {
		return err
	}
	return writeResponse(ctx.Connection, result)
}

func (c *SetCommand) DryExecute(ctx *CommandContext) (string, error) {
	var result string
	var val interface{}
	key, valStr := ctx.Args[1], ctx.Args[2]
	val = valStr
	valInt, err := utils.ToInt64(valStr)
	if err == nil {
		val = valInt
	}
	if len(ctx.Args) == 3 {
		result = ctx.Store.Set(key, val, -1)
	} else if len(ctx.Args) == 5 && ctx.Args[3] == "px" {
		expire := ctx.Args[4]
		expireTimeMs, err := strconv.Atoi(expire)
		if err != nil {
			return "", fmt.Errorf("expire time is not a number: %v", err)
		}
		result = ctx.Store.Set(key, val, expireTimeMs)
	}

	// replicate command to replicas
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, replica := range ctx.ReplicaConnections {
			_, err := replica.Write([]byte(protocol.FormatRESPBulkStringsArray(ctx.Args)))
			if err != nil {
				fmt.Printf("couldn't propogate `set` command to replica")
			}
		}
	}()
	if ctx.WaitForCommandFinish {
		wg.Wait()
	}

	return protocol.FormatRESPSimpleString(result), nil
}
