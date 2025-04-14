package commands

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/db"
	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

// IncrCommand handles Redis INCR command
type IncrCommand struct{}

func (c *IncrCommand) Name() string {
	return "incr"
}

func (c *IncrCommand) Execute(ctx *CommandContext) error {
	if len(ctx.Args) != 2 {
		return fmt.Errorf("expecting 1 argument: INCR <key>")
	}
	clientAdr := ctx.Connection.RemoteAddr().String()
	if ctx.ServerControl.IsTransactionStarted(clientAdr) {
		ctx.ServerControl.AddTransactionCommand(clientAdr, ctx)
		_, err := ctx.Connection.Write([]byte("+QUEUED\r\n"))
		if err != nil {
			return fmt.Errorf("cannot write to connection: %v", err)
		}
		return nil
	}

	res, err := c.DryExecute(ctx)
	if err != nil {
		return handleError(ctx.Connection, err)
	}
	return writeResponse(ctx.Connection, res)
}

func (c *IncrCommand) DryExecute(ctx *CommandContext) (string, error) {
	incrementedVal := int64(1)

	key := ctx.Args[1]
	val := ctx.Store.Get(key)

	if val == nil {
		_ = ctx.Store.Set(key, incrementedVal, -1)
		return protocol.FormatRESPInt(int64(incrementedVal)), nil
	}

	if val.ValueType != db.IntegerType {
		return "", fmt.Errorf("value is not an integer or out of range")
	}

	oldVal := val.ToInt64()

	incrementedVal = oldVal + 1
	_ = ctx.Store.Set(key, incrementedVal, -1)

	return protocol.FormatRESPInt(incrementedVal), nil
}
