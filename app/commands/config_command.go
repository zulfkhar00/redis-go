package commands

import (
	"fmt"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

// ConfigCommand handles Redis CONFIG command
type ConfigCommand struct{}

func (c *ConfigCommand) Name() string {
	return "config"
}

func (c *ConfigCommand) Execute(ctx *CommandContext) error {
	if len(ctx.Args) <= 2 {
		return fmt.Errorf("expecting at least 1 arguemnt for CONFIG")
	}
	res, _ := c.DryExecute(ctx)
	if err := writeResponse(ctx.Connection, res); err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}

	return nil
}

func (c *ConfigCommand) DryExecute(ctx *CommandContext) (string, error) {
	if strings.ToLower(ctx.Args[1]) == "get" && ctx.Args[2] == "dir" {
		return protocol.FormatRESPBulkStringsArray([]string{"dir", ctx.Config.Dir}), nil
	}
	if strings.ToLower(ctx.Args[1]) == "get" && ctx.Args[2] == "dbfilename" {
		return protocol.FormatRESPBulkStringsArray([]string{"dbfilename", ctx.Config.DbFileName}), nil
	}
	return "", nil
}
