package commands

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

// UnknownCommand handles unrecognized command
type UnknownCommand struct{}

func (c *UnknownCommand) Name() string {
	return "unknown"
}

func (c *UnknownCommand) Execute(ctx *CommandContext) error {
	var buf []byte
	buf = protocol.AppendSimpleString(buf, "ERR unknown command")
	_, err := ctx.Connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}

	return nil
}

func (c *UnknownCommand) DryExecute(ctx *CommandContext) (string, error) {
	return "", nil
}
