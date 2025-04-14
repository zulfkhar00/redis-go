package commands

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

// PingCommand handles Redis PING command
type PingCommand struct{}

func (c *PingCommand) Name() string {
	return "ping"
}

func (c *PingCommand) Execute(ctx *CommandContext) error {
	var buf []byte
	buf = protocol.AppendSimpleString(buf, "PONG")
	_, err := ctx.Connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}
	return nil
}

func (c *PingCommand) DryExecute(ctx *CommandContext) (string, error) {
	return "", nil
}
