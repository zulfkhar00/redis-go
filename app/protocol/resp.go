package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	Array      = '*'
	BulkString = '$'
)

// Custom errors for string integer encoding
type EightBitStringInteger struct{}

func (e EightBitStringInteger) Error() string {
	return "this is a 8 bit string integer"
}

type SixteenBitStringInteger struct{}

func (e SixteenBitStringInteger) Error() string {
	return "this is a 16 bit string integer"
}

type ThirtyTwoBitStringInteger struct{}

func (e ThirtyTwoBitStringInteger) Error() string {
	return "this is a 32 bit string integer"
}

// FormatBulkString formats a string as a RESP bulk string
func FormatBulkString(value string) string {
	if value == "" {
		// Return null bulk string format for non-existent or empty values
		return "$-1\r\n"
	}
	// Return the bulk string format: $<length>\r\n<value>\r\n
	return "$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n"
}

// FormatRESPArray formats a string slice as a RESP array
func FormatRESPArray(elems []string) string {
	resp := fmt.Sprintf("*%d\r\n", len(elems))
	for _, elem := range elems {
		resp += FormatBulkString(elem)
	}
	return resp
}

// AppendSimpleString appends a RESP simple string to a byte slice
func AppendSimpleString(buf []byte, str string) []byte {
	return fmt.Appendf(buf, "+%s\r\n", str)
}

// ParseCommand parses a RESP command from a byte slice
func ParseCommand(buf []byte) ([]string, error) {
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
	arrayLen, i, err := ParseNumber(buf, i)
	if err != nil {
		return nil, err
	}

	var args []string
	var arg string
	for a := 0; a < arrayLen; a++ {
		arg, i, err = ParseString(buf, i)
		if err != nil {
			return nil, err
		}

		args = append(args, arg)
	}

	return args, nil
}

// ReadRedisCommand reads a complete Redis command from a bufio.Reader
func ReadRedisCommand(reader *bufio.Reader) ([]string, int, error) {
	bytesProcessed := 0
	// Read the first line which should be the array marker
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, 0, err
	}
	bytesProcessed += len(line)
	line = strings.TrimSpace(line)

	// Check if this is an array
	if !strings.HasPrefix(line, "*") {
		return nil, 0, fmt.Errorf("expected array, got: %s", line)
	}

	// Parse number of elements in the array
	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, 0, fmt.Errorf("invalid array length: %s", line[1:])
	}

	// Read each element in the command
	cmd := make([]string, count)
	for i := 0; i < count; i++ {
		// Read bulk string marker
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, 0, err
		}
		bytesProcessed += len(line)
		line = strings.TrimSpace(line)

		if !strings.HasPrefix(line, "$") {
			return nil, 0, fmt.Errorf("expected bulk string, got: %s", line)
		}

		// Parse string length
		strLen, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, 0, fmt.Errorf("invalid string length: %s", line[1:])
		}

		// Read exactly strLen bytes
		value := make([]byte, strLen)
		n, err := io.ReadFull(reader, value)
		if err != nil {
			return nil, 0, err
		}
		bytesProcessed += n

		// Read the trailing \r\n
		trainlingLine, err := reader.ReadString('\n')
		if err != nil {
			return nil, 0, err
		}
		bytesProcessed += len(trainlingLine)

		cmd[i] = string(value)
	}

	return cmd, bytesProcessed, nil
}

// Helper functions for parsing RESP protocol
func expect(buffer []byte, i int, expected string) (int, error) {
	// Check if the bytes at the given index i match the expected value
	if i+len(expected) <= len(buffer) && string(buffer[i:i+len(expected)]) == expected {
		return i + len(expected), nil
	}

	return i, fmt.Errorf("expected %q", expected)
}

func ParseString(buffer []byte, start int) (string, int, error) {
	i := start
	if i == len(buffer) {
		return "", start, io.ErrUnexpectedEOF
	}

	// check if it is string
	if buffer[i] != BulkString {
		return "", start, errors.New("array element is not bulk string")
	}
	i++

	bulkStringLen, i, err := ParseNumber(buffer, i)
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

func ParseNumber(buf []byte, start int) (n, i int, err error) {
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
