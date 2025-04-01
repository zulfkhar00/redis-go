package commands

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

// TypeCommand handles Redis TYPE command
type TypeCommand struct{}

func (c *TypeCommand) Name() string {
	return "type"
}

func (c *TypeCommand) Execute(ctx *CommandContext) error {
	if len(ctx.Args) != 2 {
		return fmt.Errorf("expecting 1 argument for TYPE")
	}
	key := ctx.Args[1]
	res := "none"
	if val := ctx.Store.Get(key); val != nil {
		res = val.ValueType.ToString()
	}

	var buf []byte
	_, err := ctx.Connection.Write([]byte(protocol.AppendSimpleString(buf, res)))
	if err != nil {
		return fmt.Errorf("error writing to connection: %v", err)
	}

	return nil
}
