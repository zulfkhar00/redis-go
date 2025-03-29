package server

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/db"
	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

type Server struct {
	cfg     *config.Config
	kvStore *db.Store
}

var replicaConnections []net.Conn
var ackChan = make(chan bool)
var isWaiting = false

func NewServer(cfg *config.Config, kvStore *db.Store) *Server {
	return &Server{
		cfg,
		kvStore,
	}
}

func (server *Server) Start() error {
	// server logic
	l, err := net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(server.cfg.Port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	for {
		connection, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			connection.Close()
			return err
		}

		go server.handleConnection(connection)
	}
}

func (server *Server) handleConnection(connection net.Conn) (err error) {
	defer connection.Close()

	reader := bufio.NewReader(connection)
	for {
		// buf := make([]byte, 1024)
		// n, err := connection.Read(buf)
		// if errors.Is(err, io.EOF) {
		// 	break
		// }

		// buf = buf[:n]

		cmd, _, err := protocol.ReadRedisCommand(reader)
		if err != nil {
			return errors.New("parse command")
		}
		fmt.Printf("[master] received cmd: %v\n", cmd)

		err = handleCommand(cmd, server, connection)
		if err != nil {
			fmt.Printf("%v\n", err)
		}
	}
}

func handleCommand(cmd []string, server *Server, connection net.Conn) error {
	switch strings.ToLower(cmd[0]) {
	case "command":
		err := handleCommandCommand(connection)
		if err != nil {
			return fmt.Errorf("handleCommandCommand error: %v", err)
		}
	case "ping":
		err := handlePingCommand(connection)
		if err != nil {
			return fmt.Errorf("handlePingCommand error: %v", err)
		}
	case "echo":
		err := handleEchoCommand(cmd, connection)
		if err != nil {
			return fmt.Errorf("handleEchoCommand error: %v", err)
		}
	case "set":
		err := handleSetCommand(cmd, server, connection)
		if err != nil {
			return fmt.Errorf("handleSetCommand error: %v", err)
		}
	case "get":
		err := handleGetCommand(cmd, server, connection)
		if err != nil {
			return fmt.Errorf("handleGetCommand error: %v", err)
		}
	case "config":
		err := handleConfigCommand(cmd, server, connection)
		if err != nil {
			return fmt.Errorf("handleConfigCommand error: %v", err)
		}
	case "keys":
		err := handleKeysCommand(cmd, server, connection)
		if err != nil {
			return fmt.Errorf("handleKeyCommand error: %v", err)
		}
	case "info":
		err := handleInfoCommand(cmd, server, connection)
		if err != nil {
			return fmt.Errorf("handleInfoCommand error: %v", err)
		}
	case "replconf":
		err := handleReplconfCommand(cmd, server, connection)
		if err != nil {
			return fmt.Errorf("handleReplconfCommand error: %v", err)
		}
	case "psync":
		err := handlePsyncCommand(cmd, server, connection)
		if err != nil {
			return fmt.Errorf("handlePsyncCommand error: %v", err)
		}
	case "wait":
		err := handleWaitCommand(cmd, server, connection)
		if err != nil {
			return fmt.Errorf("handleWaitCommand error: %v", err)
		}
	case "type":
		err := handleTypeCommand(cmd, server, connection)
		if err != nil {
			return fmt.Errorf("handleTypeCommand error: %v", err)
		}
	case "xadd":
		err := handleXaddCommand(cmd, server, connection)
		if err != nil {
			return fmt.Errorf("handleXaddCommand error: %v", err)
		}
	case "xrange":
		err := handleXrangeCommand(cmd, server, connection)
		if err != nil {
			return fmt.Errorf("handleXaddCommand error: %v", err)
		}
	default:
		err := handleUnknownCommand(connection)
		if err != nil {
			return fmt.Errorf("handleUnknownCommand error: %v", err)
		}
	}

	return nil
}

func handleCommandCommand(connection net.Conn) error {
	var buf []byte
	buf = protocol.AppendSimpleString(buf, "")
	_, err := connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}
	return nil
}

func handlePingCommand(connection net.Conn) error {
	var buf []byte
	buf = protocol.AppendSimpleString(buf, "PONG")
	_, err := connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}
	return nil
}

func handleEchoCommand(cmd []string, connection net.Conn) error {
	var buf []byte
	buf = protocol.AppendSimpleString(buf, strings.Join(cmd[1:], " "))
	_, err := connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}
	return nil
}

func handleSetCommand(cmd []string, server *Server, connection net.Conn) error {
	var buf []byte
	var result string
	if len(cmd) == 3 {
		result = server.kvStore.Set(cmd[1], cmd[2], -1)
	} else if len(cmd) == 5 && cmd[3] == "px" {
		expireTimeMs, err := strconv.Atoi(cmd[4])
		if err != nil {
			return fmt.Errorf("expire time is not a number: %v", err)
		}
		result = server.kvStore.Set(cmd[1], cmd[2], expireTimeMs)
	}
	buf = protocol.AppendSimpleString(buf, result)
	_, err := connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}

	// replicate command to replicas
	for _, replica := range replicaConnections {
		_, err = replica.Write([]byte(protocol.FormatRESPArray(cmd)))
		if err != nil {
			fmt.Printf("couldn't propogate `set` command to replica")
		}
	}

	return nil
}

func handleGetCommand(cmd []string, server *Server, connection net.Conn) error {
	if len(cmd) != 2 {
		return fmt.Errorf("expecting only 1 argument for GET")
	}
	result := server.kvStore.Get(cmd[1])
	buf := []byte(protocol.FormatBulkString(""))
	if result != nil && result.ValueType != db.StreamType {
		buf = []byte(protocol.FormatBulkString(result.ToString()))
	}
	_, err := connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}

	return nil
}

func handleConfigCommand(cmd []string, server *Server, connection net.Conn) error {
	if len(cmd) <= 2 {
		return fmt.Errorf("expecting at least 1 arguemnt for CONFIG")
	}
	var buf []byte
	if strings.ToLower(cmd[1]) == "get" && cmd[2] == "dir" {
		buf = []byte(protocol.FormatRESPArray([]string{"dir", server.cfg.Dir}))
	}
	if strings.ToLower(cmd[1]) == "get" && cmd[2] == "dbfilename" {
		buf = []byte(protocol.FormatRESPArray([]string{"dbfilename", server.cfg.DbFileName}))
	}
	_, err := connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}

	return nil
}

func handleKeysCommand(cmd []string, server *Server, connection net.Conn) error {
	if len(cmd) != 2 {
		return fmt.Errorf("expecting 1 argument for KEYS")
	}
	result := server.kvStore.Keys(cmd[1])
	buf := []byte(protocol.FormatRESPArray(result))
	_, err := connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}

	return nil
}

func handleReplconfCommand(cmd []string, server *Server, connection net.Conn) error {
	var buf []byte
	server.ReceiveReplicaConfig(cmd)
	fmt.Printf("Global master received: %v\n", cmd)
	if strings.ToLower(cmd[1]) == "getack" {
		buf = []byte(protocol.FormatRESPArray([]string{"REPLCONF", "GETACK", "*"}))
		if len(replicaConnections) == 0 {
			return fmt.Errorf("no replicas found")
		}
		// TODO: extend for all replicas
		replica := replicaConnections[0]
		_, err := replica.Write(buf)
		fmt.Printf("[inside] Master sent to replica: %v\n", []string{"REPLCONF", "GETACK", "*"})
		if err != nil {
			fmt.Printf("[inside] 1\n")
			return fmt.Errorf("cannot write to connection for GETACK: %v", err)
		}
		return nil
	}
	if strings.ToLower(cmd[1]) == "ack" {
		if len(cmd) != 3 {
			return fmt.Errorf("didn't get offset for REPLCONF ACK <offset>")
		}
		_, err := strconv.Atoi(cmd[2])
		if err != nil {
			return fmt.Errorf("REPLCONF ACK <offset>: offset is not a number")
		}
		if isWaiting {
			ackChan <- true
		}
		return nil
	}

	fmt.Printf("Outside of replconf getack\n")
	buf = protocol.AppendSimpleString(buf, "OK")
	fmt.Printf("Master sent: OK\n")
	_, err := connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}

	return nil
}

func handlePsyncCommand(cmd []string, server *Server, connection net.Conn) error {
	if len(cmd) != 3 {
		return fmt.Errorf("expecting 2 arguments for PSYNC")
	}
	server.Psync(cmd[1], cmd[2])
	fmt.Printf("Master received: %v\n", cmd)
	resp := fmt.Sprintf("FULLRESYNC %s %d", server.cfg.MasterReplID, server.cfg.MasterReplOffset)
	fmt.Printf("Master sent: %s\n", resp)
	var buf []byte
	buf = protocol.AppendSimpleString(buf, resp)
	_, err := connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}

	rdbParser := db.NewRDBParser(server.cfg.Dir, server.cfg.DbFileName)
	content, err := rdbParser.OpenRDBFile()
	if err != nil {
		return fmt.Errorf("error writing to connection: %v", err)
	}
	rdbMessage := append([]byte(fmt.Sprintf("$%d\r\n", len(content))), content...)
	fmt.Printf("Master sent: %q\n", rdbMessage)
	_, err = connection.Write(rdbMessage)
	if err != nil {
		return fmt.Errorf("error writing to connection: %v", err)
	}
	replicaConnections = append(replicaConnections, connection)

	return nil
}

func handleWaitCommand(cmd []string, server *Server, connection net.Conn) error {
	if len(cmd) != 3 {
		return fmt.Errorf("supposed to get WAIT <replica_count> <timeout>, but got: %v", cmd)
	}
	replicaCount, err := strconv.Atoi(cmd[1]) // replicaCount
	if err != nil {
		return fmt.Errorf("WAIT <replica_count> should be number, but got: %s", cmd[1])
	}
	timeoutMs, err := strconv.Atoi(cmd[2]) // timeoutMs
	if err != nil {
		return fmt.Errorf("WAIT <timeout> should be number, but got: %s", cmd[2])
	}

	if server.kvStore.Length() == 0 {
		_, err = connection.Write([]byte(protocol.FormatRESPInt(int64(len(replicaConnections)))))
		if err != nil {
			return fmt.Errorf("error writing to connection: %v", err)
		}
		return nil
	}

	isWaiting = true
	// err = handleReplconfCommand([]string{"REPLCONF", "GETACK", "*"}, server, connection)
	for _, replica := range replicaConnections {
		go func() {
			_, err = replica.Write([]byte(protocol.FormatRESPArray([]string{"REPLCONF", "GETACK", "*"})))
			if err != nil {
				fmt.Printf("[master->replica] error writing to connection: %v\n", err)
			}
		}()
	}
	if err != nil {
		fmt.Printf("[handleWaitCommand] handleReplconfCommand error: %v\n", err)
	}
	timer := time.After(time.Duration(timeoutMs) * time.Millisecond)
	ackReceived := 0
	done := false
	for !done {
		select {
		case <-ackChan:
			ackReceived++
			fmt.Printf("ack received: %d\n", ackReceived)
			if ackReceived >= replicaCount {
				done = true
			}
		case <-timer:
			fmt.Printf("timeout\n")
			done = true
		}
	}
	isWaiting = false
	_, err = connection.Write([]byte(protocol.FormatRESPInt(int64(ackReceived))))
	if err != nil {
		return fmt.Errorf("error writing to connection: %v", err)
	}

	return nil
}

func handleTypeCommand(cmd []string, server *Server, connection net.Conn) error {
	if len(cmd) != 2 {
		return fmt.Errorf("expecting 1 argument for TYPE")
	}
	key := cmd[1]
	res := "none"
	if val := server.kvStore.Get(key); val != nil {
		res = val.ValueType.ToString()
	}

	var buf []byte
	_, err := connection.Write([]byte(protocol.AppendSimpleString(buf, res)))
	if err != nil {
		return fmt.Errorf("error writing to connection: %v", err)
	}

	return nil
}

func handleXaddCommand(cmd []string, server *Server, connection net.Conn) error {
	if len(cmd) < 5 {
		return fmt.Errorf("expecting at least 5 arguments for XADD: XADD <stream_key> <entryID> <key> <val>")
	}
	fields := make(map[string]string)
	for i := 3; i < len(cmd)-1; i++ {
		key, val := cmd[i], cmd[i+1]
		fields[key] = val
	}
	res, err := server.kvStore.SetStream(cmd[1], cmd[2], fields)
	if err != nil {
		_, err := connection.Write([]byte(protocol.FormatRESPError(err)))
		if err != nil {
			return fmt.Errorf("error writing to connection: %v", err)
		}
		return nil
	}

	_, err = connection.Write([]byte(protocol.FormatBulkString(res)))
	if err != nil {
		return fmt.Errorf("error writing to connection: %v", err)
	}

	return nil
}

func handleXrangeCommand(cmd []string, server *Server, connection net.Conn) error {
	if len(cmd) != 4 {
		return fmt.Errorf("expecting 4 arguments for XRANGE: XRANGE <stream_key> <start_entry_id> <end_entry_id>")
	}
	entries, err := server.kvStore.GetRangeStreamEntries(cmd[1], cmd[2], cmd[3])
	if err != nil {
		_, err := connection.Write([]byte(protocol.FormatRESPError(err)))
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

	_, err = connection.Write([]byte(res))
	if err != nil {
		return fmt.Errorf("error writing to connection: %v", err)
	}

	return nil
}

func handleUnknownCommand(connection net.Conn) error {
	var buf []byte
	buf = protocol.AppendSimpleString(buf, "ERR unknown command")
	_, err := connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}

	return nil
}

func handleInfoCommand(cmd []string, server *Server, connection net.Conn) error {
	result := ""
	if len(cmd) == 2 {
		result = server.info(cmd[1])
	}
	buf := []byte(protocol.FormatBulkString(result))
	_, err := connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}

	return nil
}

func (server *Server) info(key string) string {
	if key == "replication" {
		res := "# Replication\n"
		res += fmt.Sprintf("role:%s\n", config.RedisInfo.ReplicationInfo.Role)
		res += fmt.Sprintf("master_replid:%s\n", config.RedisInfo.ReplicationInfo.MasterReplID)
		res += fmt.Sprintf("master_repl_offset:%d\n", config.RedisInfo.ReplicationInfo.MasterReplOffset)

		return res
	}

	return ""
}

func (server *Server) ReceiveReplicaConfig(cmd []string) {
	if len(cmd) != 3 {
		return
	}
	if strings.ToLower(cmd[1]) == "listening-port" {
		// replicaPort
		_ = cmd[2]
		return
	}
	if strings.ToLower(cmd[1]) == "capa" && strings.ToLower(cmd[2]) == "psync2" {
		// last phase of PART 2 of handshake
		return
	}
}

func (server *Server) Psync(replicationIDFromReplica, offsetFromReplica string) {
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
