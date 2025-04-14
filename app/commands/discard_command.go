package commands

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

// DiscardCommand handles Redis DISCARD command
type DiscardCommand struct{}

func (c *DiscardCommand) Name() string {
	return "discard"
}

func (c *DiscardCommand) Execute(ctx *CommandContext) error {
	clientAdr := ctx.Connection.RemoteAddr().String()
	if !ctx.ServerControl.IsTransactionStarted(clientAdr) {
		return handleError(ctx.Connection, fmt.Errorf("DISCARD without MULTI"))
	}

	ctx.ServerControl.FinishTransaction(clientAdr)
	return writeResponse(ctx.Connection, protocol.FormatRESPSimpleString("OK"))
}

func (c *DiscardCommand) DryExecute(ctx *CommandContext) (string, error) {
	return "", nil
}
