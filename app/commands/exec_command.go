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
		ctx.ServerControl.FinishTransaction(clientAdr)
		return writeResponse(ctx.Connection, protocol.FormatRESPBulkStringsArray([]string{}))
	}

	responses := make([]string, 0)

	for _, cmdCtx := range cmds {
		command := ctx.CommandRegistry.Get(cmdCtx.Args[0])
		res, err := command.DryExecute(cmdCtx)
		// fmt.Printf("%v -> %q\n", cmdCtx.Args, res)
		if err != nil {
			responses = append(responses, protocol.FormatRESPError(err))
			continue
		}
		responses = append(responses, res)
	}

	ctx.ServerControl.FinishTransaction(clientAdr)
	return writeResponse(ctx.Connection, protocol.FormatRESPArray(responses))
}

func (c *ExecCommand) DryExecute(ctx *CommandContext) (string, error) {
	return "", nil
}
