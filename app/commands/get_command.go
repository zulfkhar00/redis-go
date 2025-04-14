package commands

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/db"
	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

// GetCommand handles Redis GET command
type GetCommand struct{}

func (c *GetCommand) Name() string {
	return "get"
}

func (c *GetCommand) Execute(ctx *CommandContext) error {
	if len(ctx.Args) != 2 {
		return fmt.Errorf("expecting only 1 argument for GET")
	}
	clientAdr := ctx.Connection.RemoteAddr().String()
	if ctx.Role.IsMaster() && ctx.ServerControl.IsTransactionStarted(clientAdr) {
		ctx.ServerControl.AddTransactionCommand(clientAdr, ctx)
		if err := writeResponse(ctx.Connection, "+QUEUED\r\n"); err != nil {
			return fmt.Errorf("cannot write to connection: %v", err)
		}
		return nil
	}

	res, _ := c.DryExecute(ctx)
	return writeResponse(ctx.Connection, res)
}

func (c *GetCommand) DryExecute(ctx *CommandContext) (string, error) {
	key := ctx.Args[1]
	result := ctx.Store.Get(key)
	resp := protocol.FormatBulkString("")
	if result != nil && result.ValueType != db.StreamType {
		if result.ValueType == db.IntegerType {
			return protocol.FormatBulkString(fmt.Sprintf("%d", result.ToInt64())), nil
		}
		return protocol.FormatBulkString(result.ToString()), nil
	}

	return resp, nil
}
