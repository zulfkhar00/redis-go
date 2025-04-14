package commands

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

// KeysCommand handles Redis KEYS command
type KeysCommand struct{}

func (c *KeysCommand) Name() string {
	return "keys"
}

func (c *KeysCommand) Execute(ctx *CommandContext) error {
	if len(ctx.Args) != 2 {
		return fmt.Errorf("expecting 1 argument for KEYS")
	}
	res, _ := c.DryExecute(ctx)
	return writeResponse(ctx.Connection, res)
}

func (c *KeysCommand) DryExecute(ctx *CommandContext) (string, error) {
	pattern := ctx.Args[1]
	result := ctx.Store.Keys(pattern)
	return protocol.FormatRESPBulkStringsArray(result), nil
}
