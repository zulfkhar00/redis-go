package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type ParsedRDBData struct {
	dbIndexToDB               map[uint64]map[string]string
	dbIndexToKeyExpireTimeMap map[uint64]map[string]string
	metadataData              map[string]string
}

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

const (
	Array      = '*'
	BulkString = '$'
)

type RedisInfo struct {
	replication ReplicationInfo
}

type ReplicationInfo struct {
	role string
}

var redisInfo RedisInfo

const defaultPort = 6379

var _ = net.Listen
var _ = os.Exit
var kvStore map[string][]string
var dir = flag.String("dir", "", "RDB directory path")
var dbfilename = flag.String("dbfilename", "", "RDB file name")
var port = flag.Int("port", defaultPort, "Port number")
var replicaOf = flag.String("replicaof", "", "Address of master/parent server")

func main() {
	fmt.Println("Logs from your program will appear here!")
	flag.Parse()

	configureInfo()

	parsedRDBData := readRDBFile()
	kvStore = computeKVStoreFromRDB(parsedRDBData)

	l, err := net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(*port))
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

func configureInfo() {
	role := "master"
	if *replicaOf != "" {
		role = "slave"
	}

	redisInfo = RedisInfo{
		replication: ReplicationInfo{
			role: role,
		},
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
		case "keys":
			result := keys(cmd)
			buf = []byte(formatRESPArray(result))
		case "info":
			result := info(cmd)
			buf = []byte(formatBulkString(result))
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

func keys(cmd []string) []string {
	requestedKeys := make([]string, 0)
	if len(cmd) < 2 {
		return requestedKeys
	}
	if cmd[1] == "*" {
		for key := range kvStore {
			requestedKeys = append(requestedKeys, key)
		}
	}

	return requestedKeys
}

func info(cmd []string) string {
	if len(cmd) == 1 {
		// all infos
		return ""
	}
	if cmd[1] == "replication" {
		res := "# Replication"
		res += fmt.Sprintf("role:%s", redisInfo.replication.role)

		return res
	}

	return ""
}

func computeKVStoreFromRDB(parsedRDBData *ParsedRDBData) map[string][]string {
	cache := make(map[string][]string)
	cacheFromRDB := parsedRDBData.dbIndexToDB[0]
	keyToExpireTime := parsedRDBData.dbIndexToKeyExpireTimeMap[0]
	for key, val := range cacheFromRDB {
		if _, exists := keyToExpireTime[key]; !exists {
			cache[key] = []string{val, "-1"}
			continue
		}

		expireTimeStr := keyToExpireTime[key]
		if expireTimeStr[len(expireTimeStr)-2:] == "ms" {
			expireTimeStr = expireTimeStr[:len(expireTimeStr)-2]
			expireTime := unixMsTimestampToTime(expireTimeStr)
			cache[key] = []string{val, millisecondsSince1970(expireTime)}
		} else if expireTimeStr[len(expireTimeStr)-1:] == "s" {
			expireTimeStr = expireTimeStr[:len(expireTimeStr)-1]
			expireTime := unixSecTimestampToTime(expireTimeStr)
			cache[key] = []string{val, millisecondsSince1970(expireTime)}
		}
	}

	return cache
}

func unixMsTimestampToTime(unixTimestamp string) time.Time {
	// Convert the string to an int64 (milliseconds)
	expireTimestampMS, err := strconv.ParseInt(unixTimestamp, 10, 64)
	if err != nil {
		log.Fatal("Error converting timestamp string:", err)
	}

	// Convert milliseconds to seconds and nanoseconds
	seconds := expireTimestampMS / 1000
	nanoseconds := (expireTimestampMS % 1000) * 1000000

	return time.Unix(seconds, nanoseconds)
}

func unixSecTimestampToTime(unixTimestamp string) time.Time {
	// Convert the string to an int64 (seconds)
	expireTimestampSec, err := strconv.ParseInt(unixTimestamp, 10, 64)
	if err != nil {
		log.Fatal("Error converting timestamp string:", err)
	}

	return time.Unix(expireTimestampSec, 0) // 0 nanoseconds
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

func readRDBFile() *ParsedRDBData {
	if (*dir == "" && *dbfilename != "") || (*dir != "" && *dbfilename == "") {
		return &ParsedRDBData{}
	}
	if *dir == "" && *dbfilename == "" {
		return &ParsedRDBData{}
	}

	filepath := *dir
	if filepath[len(filepath)-1] != '/' {
		filepath += "/"
	}
	filepath += *dbfilename

	// check if file exists
	_, err := os.Stat(filepath)
	if os.IsNotExist(err) {
		return &ParsedRDBData{}
	}

	file, err := os.Open(filepath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Parse the RDB file content
	parsedRDBData, err := parseRDBData(file)
	if err != nil {
		log.Fatalf("Error parsing RDB file: %v", err)
		return &ParsedRDBData{}
	}

	return parsedRDBData
}

func parseRDBData(file *os.File) (*ParsedRDBData, error) {
	err := parseHeaderSection(file)
	if err != nil {
		return nil, err
	}

	metadataData, err := parseMetadataSection(file)
	if err != nil {
		return nil, err
	}

	dbIndexToDB, dbIndexToKeyExpireTimeMap, err := parseDBSection(file)
	if err != nil {
		return nil, err
	}

	return &ParsedRDBData{
		dbIndexToDB:               dbIndexToDB,
		dbIndexToKeyExpireTimeMap: dbIndexToKeyExpireTimeMap,
		metadataData:              metadataData,
	}, nil
}

func parseDBSection(file *os.File) (dbIndexToDB map[uint64]map[string]string,
	dbIndexToKeyExpireTimeMap map[uint64]map[string]string, err error) {
	dbIndexToDB = make(map[uint64]map[string]string)
	dbIndexToKeyExpireTimeMap = make(map[uint64]map[string]string)

	firstDbSection := true
	for {
		db := make(map[string]string)
		dbKeyExpireTime := make(map[string]string)

		if !firstDbSection {
			firstByte := make([]byte, 1)
			_, err := file.Read(firstByte)
			if err != nil {
				return nil, nil, err
			}

			if firstByte[0] == 0xFF {
				// end of file
				break
			}

			if firstByte[0] != 0xFE {
				// should be start of Database subsection
				return nil, nil, fmt.Errorf("should be start of Database subsection: first byte is not 0xFE")
			}
		}

		// read Database subsection
		dbIndex, err := decodeSizeEncoding(file)
		if err != nil {
			return nil, nil, fmt.Errorf("Error reading Database subsection index: %v", err)
		}
		hashTableSizeFlag := make([]byte, 1)
		_, err = file.Read(hashTableSizeFlag)
		if err != nil {
			return nil, nil, err
		}
		if hashTableSizeFlag[0] != 0xFB {
			return nil, nil, fmt.Errorf("after DB index there is no hashtable size flag: 0xFB")
		}

		hashTableSize, err := decodeSizeEncoding(file)
		if err != nil {
			return nil, nil, fmt.Errorf("Error reading Database subsection hash table size: %v", err)
		}
		expiredKeySize, err := decodeSizeEncoding(file) // number of expiry keys
		fmt.Printf("expired key size: %d\n", expiredKeySize)
		if err != nil {
			return nil, nil, fmt.Errorf("Error reading Database subsection expire hash table size: %v", err)
		}

		var i uint64
		for i = 0; i < hashTableSize; i++ {
			// parse key value section
			firstByte := make([]byte, 1)
			_, err := file.Read(firstByte)
			if err != nil {
				return nil, nil, err
			}
			var unixExpireTimeMs, unixExpireTimeSec uint64
			curByteIsFlag := false
			if firstByte[0] == 0xFC {
				curByteIsFlag = true
				// expiration in milliseconds
				exipreTimeBytes := make([]byte, 8)
				_, err = file.Read(exipreTimeBytes)
				if err != nil {
					return nil, nil, err
				}
				unixExpireTimeMs = binary.LittleEndian.Uint64(exipreTimeBytes)
			}
			if firstByte[0] == 0xFD {
				curByteIsFlag = true
				// expiration in seconds
				exipreTimeBytes := make([]byte, 4)
				_, err = file.Read(exipreTimeBytes)
				if err != nil {
					return nil, nil, err
				}
				unixExpireTimeSec = binary.LittleEndian.Uint64(exipreTimeBytes)
			}

			var key, val string

			if curByteIsFlag {
				_, err = file.Read(firstByte)
				if err != nil {
					return nil, nil, err
				}
			}
			valueTypeEncodingFlag := firstByte[0]
			if valueTypeEncodingFlag == 0x00 {
				// value is string encoded
				key = decodeStringEncoding(file)
				val = decodeStringEncoding(file)
			} else {
				// unsupported value encoding: int/map/list etc.
				return nil, nil, fmt.Errorf("unsupported value encoding")
			}

			db[key] = val
			if unixExpireTimeMs == 0 && unixExpireTimeSec != 0 {
				dbKeyExpireTime[key] = fmt.Sprintf("%ds", unixExpireTimeSec)
			}
			if unixExpireTimeMs != 0 && unixExpireTimeSec == 0 {
				dbKeyExpireTime[key] = fmt.Sprintf("%dms", unixExpireTimeMs)
			}
		}

		dbIndexToDB[dbIndex] = db
		dbIndexToKeyExpireTimeMap[dbIndex] = dbKeyExpireTime
		firstDbSection = false
	}

	// TODO:
	// 1. read 8-byte CRC64 checksum of the entire file
	// 2. compute entire file checksum and compare both checksums
	checksum := make([]byte, 8)
	_, err = file.Read(checksum)
	if err != nil {
		return nil, nil, err
	}

	return dbIndexToDB, dbIndexToKeyExpireTimeMap, nil
}

func parseHeaderSection(file *os.File) error {
	// Read the magic header to confirm it's a valid RDB file
	magicBytes := make([]byte, 9) // "REDIS" + version byte
	_, err := file.Read(magicBytes)
	if err != nil {
		return err
	}
	if string(magicBytes[:5]) != "REDIS" || string(magicBytes[5:]) != "0011" {
		return fmt.Errorf("not a valid RDB file")
	}
	return nil
}

func parseMetadataSection(file *os.File) (map[string]string, error) {
	metadata := make(map[string]string)

	for {
		metadataSectionStart := make([]byte, 1)
		_, err := file.Read(metadataSectionStart)
		if err != nil {
			return nil, err
		}
		if metadataSectionStart[0] == 0xFE {
			// Database start flag
			break
		}
		if metadataSectionStart[0] != 0xFA {
			return nil, fmt.Errorf("no meatdata header flag: 0xFE")
		}
		metadataKey := decodeStringEncoding(file)
		metadataVal := decodeStringEncoding(file)
		metadata[metadataKey] = metadataVal
	}

	return metadata, nil
}

func decodeStringEncoding(file *os.File) string {
	stringSize, err := decodeSizeEncoding(file)
	if err != nil {
		switch err.(type) {
		case EightBitStringInteger:
			secondByte := make([]byte, 1)
			_, err = file.Read(secondByte)
			if err != nil {
				return "0"
			}
			return strconv.Itoa(int(secondByte[0]))
		case *SixteenBitStringInteger:
			nextTwoBytes := make([]byte, 2)
			_, err = file.Read(nextTwoBytes)
			if err != nil {
				return "0"
			}
			return strconv.FormatUint(binary.LittleEndian.Uint64(nextTwoBytes), 10)
		case *ThirtyTwoBitStringInteger:
			nextThreeBytes := make([]byte, 3)
			_, err = file.Read(nextThreeBytes)
			if err != nil {
				return "0"
			}
			return strconv.FormatUint(binary.LittleEndian.Uint64(nextThreeBytes), 10)
		default:
			return "0"
		}
	}
	strBytes := make([]byte, stringSize)
	_, err = file.Read(strBytes)
	if err != nil {
		return ""
	}

	return string(strBytes)
}

func decodeSizeEncoding(file *os.File) (uint64, error) {
	firstByte := make([]byte, 1)
	_, err := file.Read(firstByte)
	if err != nil {
		return 0, err
	}
	firstTwoBits := firstByte[0] >> 6
	if firstTwoBits == 0b00 {
		return uint64(firstByte[0]), nil
	}
	if firstTwoBits == 0b01 {
		sixBitFirstByte := (firstByte[0] << 2) >> 2
		secondByte := make([]byte, 1)
		_, err = file.Read(secondByte)
		if err != nil {
			return 0, err
		}
		size := uint64((uint16(sixBitFirstByte) << 8) | uint16(secondByte[0]))
		return size, nil
	}
	if firstTwoBits == 0b10 {
		sizeBytes := make([]byte, 4)
		_, err = file.Read(sizeBytes)
		if err != nil {
			return 0, err
		}
		return binary.BigEndian.Uint64(sizeBytes), nil
	}
	if firstTwoBits == 0b11 {
		// 0x3F is 0b00111111
		stringEncodingType := firstByte[0]
		var value uint64

		switch stringEncodingType {
		case 0xC0:
			// 8 bit string integer
			return 0, EightBitStringInteger{}
		case 0xC1:
			// 16-bit integer string encoding (little-endian)
			return 0, SixteenBitStringInteger{}

		case 0xC2:
			// 32-bit integer string encoding (little-endian)
			return 0, ThirtyTwoBitStringInteger{}
		default:
			// 0xC3 string encoding type means that the string is compressed with the LZF algorithm
			// skipping this case
			value = 0
		}

		return value, nil
	}

	return 0, fmt.Errorf("cannot understand size encoding")
}
