package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/db"
	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

type Replica struct {
	cfg     *config.Config
	kvStore *db.Store
}

func NewReplica(cfg *config.Config, kvStore *db.Store) *Replica {
	return &Replica{
		cfg,
		kvStore,
	}
}

func (server *Replica) Start() error {
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

func (server *Replica) handleConnection(connection net.Conn) (err error) {
	defer connection.Close()
	fmt.Printf("Replica: starting to process client commands\n")

	reader := bufio.NewReader(connection)
	for {
		cmd, _, err := protocol.ReadRedisCommand(reader)
		if err != nil {
			return errors.New("parse command")
		}

		err = handleReplicaCommand(cmd, server, connection)
		if err != nil {
			fmt.Printf("%v\n", err)
		}
	}
}

func handleReplicaCommand(cmd []string, server *Replica, connection net.Conn) error {
	switch strings.ToLower(cmd[0]) {
	case "command":
		err := handleCommandReplicaCommand(connection)
		if err != nil {
			return fmt.Errorf("handleCommandReplicaCommand error: %v", err)
		}
	case "ping":
		err := handlePingReplicaCommand(connection)
		if err != nil {
			return fmt.Errorf("handlePingReplicaCommand error: %v", err)
		}
	case "echo":
		err := handleEchoReplicaCommand(cmd, connection)
		if err != nil {
			return fmt.Errorf("handleEchoReplicaCommand error: %v", err)
		}
	case "set":
		err := handleSetReplicaCommand(cmd, server, connection)
		if err != nil {
			return fmt.Errorf("handleSetReplicaCommand error: %v", err)
		}
	case "get":
		err := handleGetReplicaCommand(cmd, server, connection)
		if err != nil {
			return fmt.Errorf("handleGetReplicaCommand error: %v", err)
		}
	case "config":
		err := handleConfigReplicaCommand(cmd, server, connection)
		if err != nil {
			return fmt.Errorf("handleConfigReplicaCommand error: %v", err)
		}
	case "keys":
		err := handleKeysReplicaCommand(cmd, server, connection)
		if err != nil {
			return fmt.Errorf("handleKeysReplicaCommand error: %v", err)
		}
	case "info":
		err := handleInfoReplicaCommand(cmd, server, connection)
		if err != nil {
			return fmt.Errorf("handleInfoReplicaCommand error: %v", err)
		}
	default:
		err := handleUnknownReplicaCommand(connection)
		if err != nil {
			return fmt.Errorf("handleUnknownCommand error: %v", err)
		}
	}

	return nil
}

func handleCommandReplicaCommand(connection net.Conn) error {
	var buf []byte
	buf = protocol.AppendSimpleString(buf, "")
	_, err := connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}
	return nil
}

func handlePingReplicaCommand(connection net.Conn) error {
	var buf []byte
	buf = protocol.AppendSimpleString(buf, "PONG")
	_, err := connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}
	return nil
}

func handleEchoReplicaCommand(cmd []string, connection net.Conn) error {
	var buf []byte
	buf = protocol.AppendSimpleString(buf, strings.Join(cmd[1:], " "))
	_, err := connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}
	return nil
}

func handleSetReplicaCommand(cmd []string, server *Replica, connection net.Conn) error {
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

	return nil
}

func handleGetReplicaCommand(cmd []string, server *Replica, connection net.Conn) error {
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

func handleConfigReplicaCommand(cmd []string, server *Replica, connection net.Conn) error {
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

func handleKeysReplicaCommand(cmd []string, server *Replica, connection net.Conn) error {
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

func handleUnknownReplicaCommand(connection net.Conn) error {
	var buf []byte
	buf = protocol.AppendSimpleString(buf, "ERR unknown command")
	_, err := connection.Write(buf)
	if err != nil {
		return fmt.Errorf("cannot write to connection: %v", err)
	}

	return nil
}

func handleInfoReplicaCommand(cmd []string, server *Replica, connection net.Conn) error {
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

func (server *Replica) ConnectToMaster() error {
	masterConn, reader, err := server.sendHandshake()
	if err != nil {
		fmt.Printf("Error sending handshake to master: %v\n", err)
		return err
	}
	go server.processReplicationCommands(masterConn, reader)

	return nil
}

func (server *Replica) sendHandshake() (net.Conn, *bufio.Reader, error) {
	// HANDSHAKE:
	// PART 1: send `PING to master`
	// PART 2a: send `REPLCONF listening-port <PORT>`
	// PART 2b: send `REPLCONF capa psync2`
	// PART 3: send `PSYNC <replicationID> <masterOffset>`
	// PART 4: accept RDB file

	// PART 1
	parts := strings.Split(server.cfg.MasterHostAndPort, " ")
	if len(parts) != 2 {
		os.Exit(0)
	}
	masterHost, masterPort := parts[0], parts[1]
	masterAddr := masterHost + ":" + masterPort
	masterConn, err := net.Dial("tcp", masterAddr)
	reader := bufio.NewReader(masterConn)
	// connection.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		return nil, nil, fmt.Errorf("tried to connect to master node on %s", masterAddr)
	}
	_, err = masterConn.Write([]byte(protocol.FormatRESPArray([]string{"PING"})))
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't send PING to master node")
	}

	// read response: should get PONG
	pongLine, err := reader.ReadString('\n')
	if errors.Is(err, io.EOF) {
		return nil, nil, err
	}
	pongLine = strings.TrimSpace(pongLine)
	if pongLine != "+PONG" {
		return nil, nil, fmt.Errorf("unexpected response, at PART1: %s", pongLine)
	}

	// PART 2a
	replConfCmds1 := []string{"REPLCONF", "listening-port", fmt.Sprint(server.cfg.Port)}
	_, err = masterConn.Write([]byte(protocol.FormatRESPArray(replConfCmds1)))
	if err != nil {
		return nil, nil, err
	}
	okLine, err := reader.ReadString('\n')
	if errors.Is(err, io.EOF) {
		return nil, nil, err
	}
	okLine = strings.TrimSpace(okLine)
	if okLine != "+OK" {
		return nil, nil, fmt.Errorf("unexpected response, at PART2a: %s", okLine)
	}

	// PART 2b
	replConfCmds2 := []string{"REPLCONF", "capa", "psync2"}
	_, err = masterConn.Write([]byte(protocol.FormatRESPArray(replConfCmds2)))
	if err != nil {
		return nil, nil, err
	}

	okLine, err = reader.ReadString('\n')
	if errors.Is(err, io.EOF) {
		return nil, nil, err
	}
	okLine = strings.TrimSpace(okLine)
	if okLine != "+OK" {
		return nil, nil, fmt.Errorf("unexpected response, at PART2b: %s", okLine)
	}

	// PART 3
	psyncCmds := []string{"PSYNC"}
	if server.cfg.MasterReplID == "" {
		// first time connecting to master
		psyncCmds = append(psyncCmds, "?")
	} else {
		psyncCmds = append(psyncCmds, fmt.Sprint(server.cfg.MasterReplID))
	}
	psyncCmds = append(psyncCmds, fmt.Sprint(server.cfg.MasterReplOffset))
	_, err = masterConn.Write([]byte(protocol.FormatRESPArray(psyncCmds)))
	if err != nil {
		return nil, nil, err
	}

	fullSyncLine, err := reader.ReadString('\n')
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read FULLRESYNC: %v", err)
	}
	fullSyncLine = strings.TrimSpace(fullSyncLine)
	if !strings.HasPrefix(fullSyncLine, "+FULLRESYNC") {
		return nil, nil, fmt.Errorf("expected FULLRESYNC, got: %s", fullSyncLine)
	}
	parts = strings.Split(fullSyncLine, " ")
	if len(parts) != 3 {
		return nil, nil, fmt.Errorf("invalid FULLRESYNC format: %s", fullSyncLine)
	}
	server.cfg.MasterReplID = parts[1] // Store the replication ID

	// PART 4
	rdbSizeLine, err := reader.ReadString('\n')
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read RDB size: %v", err)
	}
	rdbSizeLine = strings.TrimSpace(rdbSizeLine)
	if !strings.HasPrefix(rdbSizeLine, "$") {
		return nil, nil, fmt.Errorf("expected RDB size marker ($), got: %s", rdbSizeLine)
	}
	rdbSize, err := strconv.Atoi(rdbSizeLine[1:])
	if err != nil {
		return nil, nil, fmt.Errorf("invalid RDB size: %s", rdbSizeLine[1:])
	}
	rdbData := make([]byte, rdbSize)
	_, err = io.ReadFull(reader, rdbData)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read RDB data: %v", err)
	}

	return masterConn, reader, nil
}

func (server *Replica) processReplicationCommands(masterConn net.Conn, reader *bufio.Reader) {
	defer masterConn.Close()
	server.cfg.MasterReplOffset = 0

	fmt.Printf("Replica: Starting to process replication commands\n")
	for {
		// fmt.Printf("Replica: Waiting for next command from master...\n")
		cmd, bytesProcessed, err := protocol.ReadRedisCommand(reader)
		if err != nil {
			fmt.Printf("[replication conn] ReadRedisCommand err: %v\n", err)
			return
		}

		if len(cmd) == 0 {
			continue
		}
		fmt.Printf("[replication conn] received cmd: %v\n", cmd)

		switch strings.ToLower(cmd[0]) {
		case "ping":
			// master alive
		case "set":
			if len(cmd) == 3 {
				_ = server.kvStore.Set(cmd[1], cmd[2], -1)
			} else if len(cmd) == 4 {
				expireTimeMs, err := strconv.Atoi(cmd[3])
				if err != nil {
					fmt.Printf("expire time is not a number")
				}
				_ = server.kvStore.Set(cmd[1], cmd[2], expireTimeMs)
			}
		case "replconf":
			if len(cmd) >= 3 && strings.ToLower(cmd[1]) == "getack" {
				ackCmd := []string{"REPLCONF", "ACK", fmt.Sprintf("%d", server.cfg.MasterReplOffset)}
				respData := protocol.FormatRESPArray(ackCmd)
				_, err := masterConn.Write([]byte(respData))
				if err != nil {
					fmt.Printf("Replica: Error sending ACK: %v\n", err)
				}
				fmt.Printf("[replication conn] sent: %q\n", []byte(respData))
			}
		default:
			fmt.Printf("Replica got unknown replication command: %v\n", cmd)
		}
		server.cfg.MasterReplOffset += bytesProcessed
	}
}

func (server *Replica) info(key string) string {
	if key == "replication" {
		res := "# Replication\n"
		res += fmt.Sprintf("role:%s\n", config.RedisInfo.ReplicationInfo.Role)
		res += fmt.Sprintf("master_replid:%s\n", config.RedisInfo.ReplicationInfo.MasterReplID)
		res += fmt.Sprintf("master_repl_offset:%d\n", config.RedisInfo.ReplicationInfo.MasterReplOffset)

		return res
	}

	return ""
}
