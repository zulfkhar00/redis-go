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

type Server struct {
	cfg     *config.Config
	kvStore *db.Store
}

// master specific variables
var replicaConnections []net.Conn

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
	for {
		buf := make([]byte, 1024)
		n, err := connection.Read(buf)
		if errors.Is(err, io.EOF) {
			break
		}

		buf = buf[:n]

		cmd, err := protocol.ParseCommand(buf)
		if err != nil {
			return errors.New("parse command")
		}

		err = handleCommand(cmd, server, connection)
		if err != nil {
			fmt.Printf("%v\n", err)
		}
	}

	return nil
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
			return fmt.Errorf("handleInfoCommand error: %v\n", err)
		}
	case "replconf":
		err := handleReplconfCommand(cmd, server, connection)
		if err != nil {
			return fmt.Errorf("handleReplconfCommand error: %v\n", err)
		}
	case "psync":
		err := handlePsyncCommand(cmd, server, connection)
		if err != nil {
			return fmt.Errorf("handlePsyncCommand error: %v\n", err)
		}
	default:
		err := handleUnknownCommand(connection)
		if err != nil {
			return fmt.Errorf("handleUnknownCommand error: %v\n", err)
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
	if server.cfg.Role == "master" {
		for _, replica := range replicaConnections {
			_, err = replica.Write([]byte(protocol.FormatRESPArray(cmd)))
			if err != nil {
				fmt.Printf("couldn't propogate `set` command to replica")
			}
		}
	}

	return nil
}

func handleGetCommand(cmd []string, server *Server, connection net.Conn) error {
	if len(cmd) != 2 {
		return fmt.Errorf("expecting only 1 argument for GET")
	}
	result := server.kvStore.Get(cmd[1])
	buf := []byte(protocol.FormatBulkString(result))
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
	server.ReceiveReplicaConfig(cmd)
	fmt.Printf("Master received: %v\n", cmd)
	var buf []byte
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

	if server.cfg.Role == "master" {
		rdbParser := db.NewRDBParser(server.cfg.Dir, server.cfg.DbFileName)
		content, err := rdbParser.OpenRDBFile()
		if err != nil {
			return fmt.Errorf("error writing to connection: %v", err)
		}
		rdbMessage := append([]byte(fmt.Sprintf("$%d\r\n", len(content))), content...)
		fmt.Printf("Master sent: %s\n", rdbMessage)
		_, err = connection.Write(rdbMessage)
		if err != nil {
			return fmt.Errorf("error writing to connection: %v", err)
		}
		replicaConnections = append(replicaConnections, connection)
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

func (server *Server) ConnectToMaster() error {
	masterConn, err := server.sendHandshake()
	if err != nil {
		fmt.Printf("Error sending handshake to master: %v\n", err)
		return err
	}
	go server.processReplicationCommands(masterConn)

	return nil
}

func (server *Server) sendHandshake() (net.Conn, error) {
	// check if I am replica
	if server.cfg.Role == "master" {
		return nil, nil
	}

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
	// connection.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		return nil, fmt.Errorf("tried to connect to master node on %s", masterAddr)
	}
	_, err = masterConn.Write([]byte(protocol.FormatRESPArray([]string{"PING"})))
	if err != nil {
		return nil, fmt.Errorf("couldn't send PING to master node")
	}

	// read response: should get PONG
	buf := make([]byte, 30)
	n, err := masterConn.Read(buf)
	if errors.Is(err, io.EOF) {
		return nil, err
	}
	buf = buf[:n]
	fmt.Printf("Replica received: %v\n", string(buf))
	resp := strings.TrimSpace(string(buf))
	if resp != "+PONG" {
		return nil, fmt.Errorf("unexpected response, at PART1: %s", resp)
	}

	// PART 2a
	replConfCmds1 := []string{"REPLCONF", "listening-port", fmt.Sprint(server.cfg.Port)}
	fmt.Printf("Replica sent: %v\n", replConfCmds1)
	_, err = masterConn.Write([]byte(protocol.FormatRESPArray(replConfCmds1)))
	if err != nil {
		return nil, err
	}
	buf = make([]byte, 100)
	n, err = masterConn.Read(buf)
	if errors.Is(err, io.EOF) {
		return nil, err
	}
	buf = buf[:n]
	fmt.Printf("Replica received: %v\n", string(buf))
	resp = strings.TrimSpace(string(buf))
	if resp != "+OK" {
		return nil, fmt.Errorf("unexpected response, at PART2a: %s", resp)
	}

	// PART 2b
	replConfCmds2 := []string{"REPLCONF", "capa", "psync2"}
	fmt.Printf("Replica sent: %v\n", replConfCmds2)
	_, err = masterConn.Write([]byte(protocol.FormatRESPArray(replConfCmds2)))
	if err != nil {
		return nil, err
	}
	buf = make([]byte, 100)
	n, err = masterConn.Read(buf)
	if errors.Is(err, io.EOF) {
		return nil, err
	}
	buf = buf[:n]
	fmt.Printf("Replica received: %v\n", string(buf))
	resp = strings.TrimSpace(string(buf))
	if resp != "+OK" {
		return nil, fmt.Errorf("unexpected response, at PART2b: %s", resp)
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
	fmt.Printf("Replica sent: %v\n", psyncCmds)
	_, err = masterConn.Write([]byte(protocol.FormatRESPArray(psyncCmds)))
	if err != nil {
		return nil, err
	}
	buf = make([]byte, 100)
	n, err = masterConn.Read(buf)
	if errors.Is(err, io.EOF) {
		return nil, err
	}
	buf = buf[:n]
	fmt.Printf("Replica received: %v\n", string(buf))
	resp = strings.TrimSpace(string(buf))
	parts = strings.Split(resp, " ")
	if !(len(parts) == 3 && parts[0] == "+FULLRESYNC") {
		return nil, fmt.Errorf("unexpected response, at PART3: %s", resp)
	}
	// TODO: replicationID
	_ = parts[1]

	// PART 4
	buf = make([]byte, 100)
	n, err = masterConn.Read(buf)
	if errors.Is(err, io.EOF) {
		fmt.Printf("Replica read EOF form master")
		return nil, err
	}
	buf = buf[:n]
	fmt.Printf("Replica received: %s\n", string(buf))

	return masterConn, nil
}

func (server *Server) processReplicationCommands(masterConn net.Conn) {
	defer masterConn.Close()

	reader := bufio.NewReader(masterConn)
	for {
		cmd, err := protocol.ReadRedisCommand(reader)
		if err != nil {
			return
		}

		switch strings.ToLower(cmd[0]) {
		case "set":
			var result string
			if len(cmd) == 3 {
				result = server.kvStore.Set(cmd[1], cmd[2], -1)
			} else if len(cmd) == 4 {
				expireTimeMs, err := strconv.Atoi(cmd[3])
				if err != nil {
					fmt.Printf("expire time is not a number")
					return
				}
				result = server.kvStore.Set(cmd[1], cmd[2], expireTimeMs)
			}
			fmt.Printf("Replica got %v, result: %s\n", cmd, result)
		default:
			fmt.Printf("Replica got unknown replication command: %v\n", cmd)
			return
		}
	}
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
	if cmd[1] == "listening-port" {
		// replicaPort
		_ = cmd[2]
		return
	}
	if cmd[1] == "capa" && cmd[2] == "psync2" {
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
