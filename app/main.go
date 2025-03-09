package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

const (
	Array      = '*'
	BulkString = '$'
)

var _ = net.Listen
var _ = os.Exit

func main() {
	fmt.Println("Logs from your program will appear here!")

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
	buf := make([]byte, 1024)
	for {
		buf = buf[:cap(buf)]
		n, err := connection.Read(buf)
		if errors.Is(err, io.EOF) {
			break
		}

		buf = buf[:n]

		fmt.Printf("command: %s", buf)
		cmd, err := parseCommand(buf)
		if err != nil {
			return errors.New("parse command")
		}

		buf = buf[:0]

		switch strings.ToLower(cmd[0]) {
		case "command":
			buf = appendSimpleString(buf, "")
		case "ping":
			buf = appendSimpleString(buf, "PONG")
		case "echo":
			buf = appendSimpleString(buf, strings.Join(cmd[1:], " "))
		default:
			panic(cmd[0])
		}

		_, err = connection.Write(buf)
		if err != err {
			return err
		}
	}

	return nil
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
