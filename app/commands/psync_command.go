package commands

import (
	"fmt"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/db"
	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

// PsyncCommand handles Redis REPLCONF command
type PsyncCommand struct{}

func (c *PsyncCommand) Name() string {
	return "replconf"
}

func (c *PsyncCommand) Execute(ctx *CommandContext) error {
	if len(ctx.Args) != 3 {
		return fmt.Errorf("expecting 2 arguments for PSYNC")
	}
	replicationID, offet := ctx.Args[1], ctx.Args[2]
	psync(replicationID, offet)
	fmt.Printf("Master received: %v\n", ctx.Args)
	resp := fmt.Sprintf("FULLRESYNC %s %d", ctx.Config.MasterReplID, ctx.Config.MasterReplOffset)
	fmt.Printf("Master sent: %s\n", resp)
	var buf []byte
	buf = protocol.AppendSimpleString(buf, resp)
	_, err := ctx.Connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}

	rdbParser := db.NewRDBParser(ctx.Config.Dir, ctx.Config.DbFileName)
	content, err := rdbParser.OpenRDBFile()
	if err != nil {
		return fmt.Errorf("error writing to connection: %v", err)
	}
	rdbMessage := append([]byte(fmt.Sprintf("$%d\r\n", len(content))), content...)
	fmt.Printf("Master sent: %q\n", rdbMessage)
	_, err = ctx.Connection.Write(rdbMessage)
	if err != nil {
		return fmt.Errorf("error writing to connection: %v", err)
	}
	ctx.ServerControl.AddReplica(ctx.Connection)

	return nil
}

func (c *PsyncCommand) DryExecute(ctx *CommandContext) (string, error) {
	return "", nil
}

func psync(replicationIDFromReplica, offsetFromReplica string) {
	replicationID, err := strconv.ParseInt(replicationIDFromReplica, 10, 64)
	if err != nil {
		return
	}
	offset, err := strconv.ParseInt(offsetFromReplica, 10, 64)
	if err != nil {
		return
	}
	// masterReplicationID, masterOffset
	_, _ = int(replicationID), int(offset)
}
