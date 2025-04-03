package commands

import "fmt"

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

	return nil
}
