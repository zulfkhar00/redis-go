package commands

import (
	"fmt"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

// EchoCommand handles Redis ECHO command
type EchoCommand struct{}

func (c *EchoCommand) Name() string {
	return "echo"
}

func (c *EchoCommand) Execute(ctx *CommandContext) error {
	var buf []byte
	res, _ := c.DryExecute(ctx)
	buf = protocol.AppendSimpleString(buf, res)
	_, err := ctx.Connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}
	return nil
}

func (c *EchoCommand) DryExecute(ctx *CommandContext) (string, error) {
	return strings.Join(ctx.Args[1:], " "), nil
}
