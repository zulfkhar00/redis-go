package commands

import (
	"fmt"

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
	if err != nil {
		_, err := ctx.Connection.Write([]byte(protocol.FormatRESPError(err)))
		if err != nil {
			return fmt.Errorf("error writing to connection: %v", err)
		}
		return nil
	}

	res := fmt.Sprintf("*%d\r\n", len(entries))
	for _, entry := range entries {
		idFormatted := protocol.FormatBulkString(entry.GetID())
		fields := make([]string, 0)
		for key, val := range entry.GetFields() {
			fields = append(fields, key)
			fields = append(fields, val)
		}

		fieldsFormatted := protocol.FormatRESPArray(fields)

		res += fmt.Sprintf("*2\r\n%s%s", idFormatted, fieldsFormatted)
	}

	_, err = ctx.Connection.Write([]byte(res))
	if err != nil {
		return fmt.Errorf("error writing to connection: %v", err)
	}

	return nil
}
