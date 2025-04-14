package commands

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

// InfoCommand handles Redis INFO command
type InfoCommand struct{}

func (c *InfoCommand) Name() string {
	return "info"
}

func (c *InfoCommand) Execute(ctx *CommandContext) error {
	res, _ := c.DryExecute(ctx)
	return writeResponse(ctx.Connection, res)
}

func (c *InfoCommand) DryExecute(ctx *CommandContext) (string, error) {
	result := ""
	if len(ctx.Args) == 2 {
		key := ctx.Args[1]
		result = c.info(key)
	}
	return result, nil
}

func (c *InfoCommand) info(key string) string {
	if key == "replication" {
		res := "# Replication\n"
		res += fmt.Sprintf("role:%s\n", config.RedisInfo.ReplicationInfo.Role)
		res += fmt.Sprintf("master_replid:%s\n", config.RedisInfo.ReplicationInfo.MasterReplID)
		res += fmt.Sprintf("master_repl_offset:%d\n", config.RedisInfo.ReplicationInfo.MasterReplOffset)

		return protocol.FormatBulkString(res)
	}

	return ""
}
