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
	key := ctx.Args[1]
	result := ctx.Store.Get(key)
	buf := []byte(protocol.FormatBulkString(""))
	if result != nil && result.ValueType != db.StreamType {
		buf = []byte(protocol.FormatBulkString(result.ToString()))
	}
	_, err := ctx.Connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}

	return nil
}
