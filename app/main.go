package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	Array      = '*'
	BulkString = '$'
)

var _ = net.Listen
var _ = os.Exit
var kvStore map[string][]string
var dir = flag.String("dir", "", "RDB directory path")
var dbfilename = flag.String("dbfilename", "", "RDB file name")

func main() {
	fmt.Println("Logs from your program will appear here!")
	flag.Parse()
	kvStore = make(map[string][]string)

	l, err := net.Listen("tcp", "0.0.0.0:6379")
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
		}

		go handleConnection(connection)
	}
}

func handleConnection(connection net.Conn) (err error) {
	defer connection.Close()
	for {
		buf := make([]byte, 1024)
		n, err := connection.Read(buf)
		if errors.Is(err, io.EOF) {
			break
		}

		buf = buf[:n]

		cmd, err := parseCommand(buf)
		if err != nil {
			return errors.New("parse command")
		}

		buf = buf[:0]
		//

		switch strings.ToLower(cmd[0]) {
		case "command":
			buf = appendSimpleString(buf, "")
		case "ping":
			buf = appendSimpleString(buf, "PONG")
		case "echo":
			buf = appendSimpleString(buf, strings.Join(cmd[1:], " "))
		case "set":
			result := set(cmd)
			buf = appendSimpleString(buf, result)
		case "get":
			result := get(cmd)
			buf = []byte(formatBulkString(result))
		case "config":
			if len(cmd) <= 2 {
				continue
			}
			if strings.ToLower(cmd[1]) == "get" && cmd[2] == "dir" {
				buf = []byte(formatRESPArray([]string{"dir", *dir}))
			}
			if strings.ToLower(cmd[1]) == "get" && cmd[2] == "dbfilename" {
				buf = []byte(formatRESPArray([]string{"dbfilename", *dbfilename}))
			}
		default:
			buf = appendSimpleString(buf, "ERR unknown command")
		}

		_, err = connection.Write(buf)
		if err != nil {
			return err
		}
	}

	return nil
}

func set(cmd []string) string {
	if len(cmd) == 3 {
		kvStore[cmd[1]] = []string{cmd[2], "-1"}
		return "OK"
	}

	if len(cmd) == 5 && strings.ToLower(cmd[3]) == "px" {
		// check that expiration time is number
		milliseconds, err := strconv.Atoi(cmd[4])
		if err != nil {
			return "incorrect expiration time"
		}

		currentTime := time.Now()
		expireTime := currentTime.Add(time.Duration(milliseconds) * time.Millisecond)

		kvStore[cmd[1]] = []string{cmd[2], millisecondsSince1970(expireTime)}
		return "OK"
	}

	return "ERR wrong number of arguments for command"
}

func get(cmd []string) string {
	if len(cmd) != 2 {
		return "ERR wrong number of arguments for command"
	}
	valObj, ok := kvStore[cmd[1]]
	if !ok {
		return ""
	}
	val, expireTime := valObj[0], valObj[1]
	if expireTime == "-1" {
		return val
	}
	if expireTime < millisecondsSince1970(time.Now()) {
		return ""
	}
	return val
}

func formatBulkString(value string) string {
	if value == "" {
		// Return null bulk string format for non-existent or empty values
		return "$-1\r\n"
	}
	// Return the bulk string format: $<length>\r\n<value>\r\n
	return "$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n"
}

func formatRESPArray(elems []string) string {
	resp := fmt.Sprintf("*%d\r\n", len(elems))
	for _, elem := range elems {
		resp += formatBulkString(elem)
	}
	return resp
}

func millisecondsSince1970(t time.Time) string {
	seconds := t.Unix()
	milliseconds := seconds*1000 + int64(t.Nanosecond())/1000000
	return fmt.Sprint(milliseconds)
}

func appendSimpleString(buf []byte, str string) []byte {
	return fmt.Appendf(buf, "+%s\r\n", str)
}

func parseCommand(buf []byte) ([]string, error) {
	i := 0
	if i == len(buf) {
		return nil, io.EOF
	}

	// check if it is array
	if buf[i] != Array {
		return nil, errors.New("array expected")
	}
	i++
	// parse array length
	arrayLen, i, err := parseNumber(buf, i)
	if err != nil {
		return nil, err
	}

	var args []string
	var arg string
	for a := 0; a < arrayLen; a++ {
		arg, i, err = parseString(buf, i)
		if err != nil {
			return nil, err
		}

		args = append(args, arg)
	}

	return args, nil
}

func expect(buffer []byte, i int, expected string) (int, error) {
	// Check if the bytes at the given index i match the expected value
	if i+len(expected) <= len(buffer) && string(buffer[i:i+len(expected)]) == expected {
		return i + len(expected), nil
	}

	return i, fmt.Errorf("expected %q", expected)
}

func parseString(buffer []byte, start int) (string, int, error) {
	i := start
	if i == len(buffer) {
		return "", start, io.ErrUnexpectedEOF
	}

	// check if it is string
	if buffer[i] != BulkString {
		return "", start, errors.New("array element is not bulk string")
	}
	i++

	bulkStringLen, i, err := parseNumber(buffer, i)
	if err != nil {
		return "", start, err
	}
	if i+bulkStringLen >= len(buffer) {
		return "", start, io.ErrUnexpectedEOF
	}

	s := string(buffer[i : i+bulkStringLen])
	i += bulkStringLen

	i, err = expect(buffer, i, "\r\n")
	if err != nil {
		return "", start, err
	}

	return s, i, nil
}

func parseNumber(buf []byte, start int) (n, i int, err error) {
	i = start
	for i < len(buf) && buf[i] >= '0' && buf[i] <= '9' {
		n = n*10 + int(buf[i]-'0')
		i++
	}
	i, err = expect(buf, i, "\r\n")
	if err != nil {
		return -1, -1, err
	}

	return n, i, nil
}
