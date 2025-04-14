package commands

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/db"
	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

// XrangeCommand handles Redis XRANGE command
type XrangeCommand struct{}

func (c *XrangeCommand) Name() string {
	return "xrange"
}

func (c *XrangeCommand) Execute(ctx *CommandContext) error {
	if len(ctx.Args) != 4 {
		return fmt.Errorf("expecting 4 arguments for XRANGE: XRANGE <stream_key> <start_entry_id> <end_entry_id>")
	}
	streamKey, startEntryID, endEntryID := ctx.Args[1], ctx.Args[2], ctx.Args[3]
	entries, err := ctx.Store.GetRangeStreamEntries(streamKey, startEntryID, endEntryID)

	res := formatStreamEntries(entries)
	if err != nil {
		return handleError(ctx.Connection, err)
	}

	return writeResponse(ctx.Connection, res)
}

func (c *XrangeCommand) DryExecute(ctx *CommandContext) (string, error) {
	return "", nil
}

func formatStreamEntries(entries []db.StreamEntry) string {
	res := fmt.Sprintf("*%d\r\n", len(entries))
	for _, entry := range entries {
		idFormatted := protocol.FormatBulkString(entry.GetID())
		fields := make([]string, 0)
		for key, val := range entry.GetFields() {
			fields = append(fields, key)
			fields = append(fields, val)
		}

		fieldsFormatted := protocol.FormatRESPBulkStringsArray(fields)

		res += fmt.Sprintf("*2\r\n%s%s", idFormatted, fieldsFormatted)
	}
	return res
}
