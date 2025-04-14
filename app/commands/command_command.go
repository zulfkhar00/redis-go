package commands

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

// CommandCommand handles Redis COMMAND command
type CommandCommand struct{}

func (c *CommandCommand) Name() string {
	return "command"
}

func (c *CommandCommand) Execute(ctx *CommandContext) error {
	var buf []byte
	res, _ := c.DryExecute(ctx)
	buf = protocol.AppendSimpleString(buf, res)
	_, err := ctx.Connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}
	return nil
}

func (c *CommandCommand) DryExecute(ctx *CommandContext) (string, error) {
	return "", nil
}
