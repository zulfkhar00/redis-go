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
	pattern := ctx.Args[1]
	result := ctx.Store.Keys(pattern)
	buf := []byte(protocol.FormatRESPArray(result))
	_, err := ctx.Connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}

	return nil
}
