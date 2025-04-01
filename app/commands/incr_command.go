package commands

import (
	"fmt"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/db"
	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

// IncrCommand handles Redis INCR command
type IncrCommand struct{}

func (c *IncrCommand) Name() string {
	return "incr"
}

func (c *IncrCommand) Execute(ctx *CommandContext) error {
	if len(ctx.Args) != 2 {
		return fmt.Errorf("expecting 1 argument: INCR <key>")
	}
	incrementedVal := int64(1)

	key := ctx.Args[1]
	val := ctx.Store.Get(key)
	if val == nil {
		_ = ctx.Store.Set(key, fmt.Sprintf("%d", incrementedVal), -1)
		return writeResponse(ctx.Connection, protocol.FormatRESPInt(int64(incrementedVal)))
	}

	if val.ValueType != db.IntegerType {
		return handleError(ctx.Connection, fmt.Errorf("value is not an integer or out of range"))
	}

	oldValStr, _ := val.Value.(string)
	oldVal, _ := strconv.ParseInt(oldValStr, 10, 64)

	incrementedVal = oldVal + 1
	_ = ctx.Store.Set(key, fmt.Sprintf("%d", incrementedVal), -1)

	return writeResponse(ctx.Connection, protocol.FormatRESPInt(incrementedVal))
}
