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
	var buf []byte
	if strings.ToLower(ctx.Args[1]) == "get" && ctx.Args[2] == "dir" {
		buf = []byte(protocol.FormatRESPArray([]string{"dir", ctx.Config.Dir}))
	}
	if strings.ToLower(ctx.Args[1]) == "get" && ctx.Args[2] == "dbfilename" {
		buf = []byte(protocol.FormatRESPArray([]string{"dbfilename", ctx.Config.DbFileName}))
	}
	_, err := ctx.Connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}

	return nil
}
