package commands

import (
	"fmt"
	"strconv"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/db"
	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

// XreadCommand handles Redis XREAD command
type XreadCommand struct{}

func (c *XreadCommand) Name() string {
	return "xread"
}

func (c *XreadCommand) Execute(ctx *CommandContext) error {
	if len(ctx.Args) < 4 {
		return fmt.Errorf("expecting at least 4 arguments for XREAD: XREAD streams <stream_key> <entry_id>")
	}
	if ctx.Args[1] == "block" {
		timeoutMs, streamKey, entryID := ctx.Args[2], ctx.Args[4], ctx.Args[5]
		return handleXreadBlockingCommand(ctx, timeoutMs, streamKey, entryID)
	}
	if ctx.Args[1] == "streams" && len(ctx.Args)%2 != 0 {
		return fmt.Errorf("expecting even number of stream keys and entryIDs for XREAD: XREAD streams <stream_key_1> <entry_id_1> <stream_key_2> <entry_id_2> etc")
	}

	ctx.Args = ctx.Args[2:]
	streamKeys, entryIDs := make([]string, 0), make([]string, 0)
	for i := 0; i < len(ctx.Args); i++ {
		if i < len(ctx.Args)/2 {
			streamKeys = append(streamKeys, ctx.Args[i])
		} else {
			entryIDs = append(entryIDs, ctx.Args[i])
		}
	}
	streamKeyEntries := make([][]db.StreamEntry, 0)

	for i, streamKey := range streamKeys {
		minEntryID := entryIDs[i]
		entries, err := ctx.Store.GetNewerStreamEntries(streamKey, minEntryID)
		if err != nil {
			return handleError(ctx.Connection, err)
		}
		streamKeyEntries = append(streamKeyEntries, entries)
	}

	res := formatMultiStreamEntries(streamKeys, streamKeyEntries)

	return writeResponse(ctx.Connection, res)
}

func (c *XreadCommand) DryExecute(ctx *CommandContext) (string, error) {
	return "", nil
}

func handleXreadBlockingCommand(ctx *CommandContext, timeoutMs, streamKey, entryID string) error {
	timeout, err := strconv.Atoi(timeoutMs)
	if err != nil {
		_, err := ctx.Connection.Write([]byte(protocol.FormatRESPError(err)))
		if err != nil {
			return fmt.Errorf("error writing to connection: %v", err)
		}
		return err
	}
	latestEntry := ctx.Store.GetLatestStreamEntry(streamKey)
	newEntries, _ := ctx.Store.GetNewerStreamEntries(streamKey, entryID)
	if len(newEntries) > 0 {
		res := fmt.Sprintf("*1\r\n*2\r\n%s*1\r\n", protocol.FormatBulkString(streamKey))
		for _, entry := range newEntries {
			idFormatted := protocol.FormatBulkString(entry.GetID())
			fields := make([]string, 0)
			for key, val := range entry.GetFields() {
				fields = append(fields, key)
				fields = append(fields, val)
			}
			fieldsFormatted := protocol.FormatRESPBulkStringsArray(fields)
			res += fmt.Sprintf("*2\r\n%s%s", idFormatted, fieldsFormatted)
		}
		_, err = ctx.Connection.Write([]byte(res))
		if err != nil {
			return fmt.Errorf("error writing to connection: %v", err)
		}
		return err
	}

	ch := db.StreamNotifier.RegisterWaiter(streamKey)
	defer db.StreamNotifier.UnRegisterWaiter(streamKey, ch)

	timer := (<-chan time.Time)(nil)
	if timeout > 0 {
		timer = time.After(time.Duration(timeout) * time.Millisecond)
	}
	done := false
	var receviedStream *db.RedisStream
	for !done {
		select {
		case stream := <-ch:
			fmt.Printf("new stream received\n")
			newStream, ok := stream.(*db.RedisStream)
			if !ok {
				fmt.Printf("new stream is not *RedisStream, it is %v\n", stream)
				return fmt.Errorf("new stream is not *RedisStream, it is %v", stream)
			}
			receviedStream = newStream
			done = true
		case <-timer:
			fmt.Printf("timeout\n")
			done = true
		}
	}
	// send this new stream
	if receviedStream == nil {
		_, err = ctx.Connection.Write([]byte("$-1\r\n"))
		return err
	}
	entriesToSend := make([]db.StreamEntry, 0)
	if latestEntry == nil {
		entriesToSend = receviedStream.GetNewerEntries(entryID)
	} else {
		entriesToSend = receviedStream.GetNewerEntries(latestEntry.GetID())
	}

	res := fmt.Sprintf("*1\r\n*2\r\n%s*1\r\n", protocol.FormatBulkString(streamKey))
	for _, entry := range entriesToSend {
		idFormatted := protocol.FormatBulkString(entry.GetID())
		fields := make([]string, 0)
		for key, val := range entry.GetFields() {
			fields = append(fields, key)
			fields = append(fields, val)
		}

		fieldsFormatted := protocol.FormatRESPBulkStringsArray(fields)

		res += fmt.Sprintf("*2\r\n%s%s", idFormatted, fieldsFormatted)
	}

	_, err = ctx.Connection.Write([]byte(res))
	if err != nil {
		return fmt.Errorf("error writing to connection: %v", err)
	}

	return err
}

func formatMultiStreamEntries(streamKeys []string, streamKeyEntries [][]db.StreamEntry) string {
	res := fmt.Sprintf("*%d\r\n", len(streamKeyEntries))
	for i, streamKey := range streamKeys {
		entries := streamKeyEntries[i]
		res += fmt.Sprintf("*2\r\n%s*1\r\n", protocol.FormatBulkString(streamKey))

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
	}
	return res
}
