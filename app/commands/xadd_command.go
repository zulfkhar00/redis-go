package commands

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/db"
	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

// XaddCommand handles Redis XADD command
type XaddCommand struct{}

func (c *XaddCommand) Name() string {
	return "xadd"
}

func (c *XaddCommand) Execute(ctx *CommandContext) error {
	if len(ctx.Args) < 5 {
		return fmt.Errorf("expecting at least 5 arguments for XADD: XADD <stream_key> <entryID> <key> <val>")
	}
	streamKey := ctx.Args[1]
	fields := make(map[string]string)
	for i := 3; i < len(ctx.Args)-1; i++ {
		key, val := ctx.Args[i], ctx.Args[i+1]
		fields[key] = val
	}

	stream, res, err := ctx.Store.SetStream(ctx.Args[1], ctx.Args[2], fields)
	if err != nil {
		return handleError(ctx.Connection, err)
	}

	if err = writeResponse(ctx.Connection, protocol.FormatBulkString(res)); err != nil {
		return err
	}

	go db.StreamNotifier.Notify(streamKey, stream)

	return nil
}

func (c *XaddCommand) DryExecute(ctx *CommandContext) (string, error) {
	return "", nil
}
