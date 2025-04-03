package commands

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

// MultiCommand handles Redis MULTI command
type MultiCommand struct{}

func (c *MultiCommand) Name() string {
	return "multi"
}

func (c *MultiCommand) Execute(ctx *CommandContext) error {
	ctx.ServerControl.TurnMultiOn(ctx.Connection.RemoteAddr().String())
	var buf []byte
	_, err := ctx.Connection.Write(protocol.AppendSimpleString(buf, "OK"))
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}
	return nil
}
