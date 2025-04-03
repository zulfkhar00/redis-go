package commands

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

// ExecCommand handles Redis EXEC command
type ExecCommand struct{}

func (c *ExecCommand) Name() string {
	return "exec"
}

func (c *ExecCommand) Execute(ctx *CommandContext) error {
	clientAdr := ctx.Connection.RemoteAddr().String()
	if !ctx.ServerControl.IsTransactionStarted(clientAdr) {
		return handleError(ctx.Connection, fmt.Errorf("EXEC without MULTI"))
	}

	cmds := ctx.ServerControl.GetTransactionCommands(clientAdr)
	if len(cmds) == 0 {
		if err := writeResponse(ctx.Connection, protocol.FormatRESPArray([]string{})); err != nil {
			return err
		}
	}

	ctx.ServerControl.FinishTransaction(clientAdr)
	return nil
}
