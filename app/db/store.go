package db

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/utils"
	art "github.com/plar/go-adaptive-radix-tree/v2"
)

type DataType int

const (
	StringType DataType = iota
	IntegerType
	StreamType

	UnknownType
)

func (d DataType) ToString() string {
	switch d {
	case StringType:
		return "string"
	case IntegerType:
		return "integer"
	case StreamType:
		return "stream"
	case UnknownType:
		return "none"
	default:
		return "none"
	}
}

type RedisStream struct {
	entries   art.Tree
	size      int64
	lastEntry *StreamEntry
}

type StreamEntry struct {
	idTimestamp uint64
	idSequence  uint64
	fields      map[string]string
	size        int64
}

// KeyValue represents a value with an optional expiration time
type KeyValue struct {
	ValueType  DataType
	Value      interface{}
	ExpireTime string // "-1" if no expiration
}

func (val *KeyValue) ToString() string {
	valStr, ok := val.Value.(string)
	if !ok {
		return ""
	}
	return valStr
}

// Store represents the key-value store
type Store struct {
	data map[string]KeyValue
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]KeyValue),
	}
}

func (s *Store) Set(key string, val interface{}, expireMS int) string {
	// Check if expiration time is provided
	valType := getDataType(val)
	var expireTimeMs string
	if expireMS < 0 {
		expireTimeMs = "-1"
	} else {
		future := time.Now().Add(time.Duration(expireMS) * time.Millisecond)
		expireTimeMs = utils.MillisecondsSince1970(future)
	}

	s.data[key] = KeyValue{ValueType: valType, Value: val, ExpireTime: expireTimeMs}

	return "OK"
}

func (s *Store) SetStream(key, entryID string, fields map[string]string) (string, error) {
	var idTimestampStr, idSequenceStr string
	if entryID == "*" {
		idTimestampStr = fmt.Sprintf("%d", time.Now().UnixMilli())
		idSequenceStr = "0"
	} else {
		entryIDParts := strings.Split(entryID, "-")
		idTimestampStr, idSequenceStr = entryIDParts[0], entryIDParts[1]
	}

	// s.mu.Lock()
	// defer s.mu.Unlock()

	entryKey := ""
	oldVal, exists := s.data[key]
	if !exists {
		idTimestamp, idSequence, err := checkAndConvertEntryIDs(idTimestampStr, idSequenceStr, nil)
		if err != nil {
			return "", err
		}
		entryKey = fmt.Sprintf("%d-%d", idTimestamp, idSequence)

		entries := art.New()
		entry := StreamEntry{idTimestamp: uint64(idTimestamp), idSequence: uint64(idSequence), fields: fields, size: int64(len(fields))}
		entries.Insert(art.Key(entryKey), art.Value(entry))
		stream := &RedisStream{entries: entries, size: 0, lastEntry: &entry}
		s.data[key] = KeyValue{ValueType: StreamType, Value: stream, ExpireTime: "-1"}
	} else {
		stream, ok := oldVal.Value.(*RedisStream)
		if !ok {
			panic(fmt.Errorf("[SetStream] s.data[%q] is not a *RedisStream, it is %s", key, oldVal.ValueType.ToString()))
		}

		idTimestamp, idSequence, err := checkAndConvertEntryIDs(idTimestampStr, idSequenceStr, stream.lastEntry)
		if err != nil {
			return "", err
		}
		entryKey = fmt.Sprintf("%d-%d", idTimestamp, idSequence)

		entry := StreamEntry{idTimestamp: uint64(idTimestamp), idSequence: uint64(idSequence), fields: fields, size: int64(len(fields))}
		stream.entries.Insert(art.Key(entryKey), art.Value(entry))
		stream.lastEntry = &entry
	}

	return entryKey, nil
}

// checkAndConvertEntryIDs autogenerates ids (timestamp, sequence) if needed, converts to int, and returns
func checkAndConvertEntryIDs(idTimestampStr, idSequenceStr string, lastStreamEntry *StreamEntry) (int, int, error) {
	idTimestamp, err := strconv.Atoi(idTimestampStr)
	if err != nil {
		return -1, -1, fmt.Errorf("entryIDTimestamp is not a number, it's %s, err: %v", idTimestampStr, err)
	}

	// Handle sequence based on whether it's a wildcard or a number
	idSequence, err := parseSequence(idSequenceStr, idTimestamp, lastStreamEntry)
	if err != nil {
		return -1, -1, err
	}

	// Validate the resulting ID pair
	if err := validateEntryID(idTimestamp, idSequence, lastStreamEntry); err != nil {
		return -1, -1, err
	}

	return idTimestamp, idSequence, nil
}

// parseSequence determines the sequence number based on input and stream state
func parseSequence(idSequenceStr string, idTimestamp int, lastStreamEntry *StreamEntry) (int, error) {
	// If sequence is not a wildcard, parse it as a number
	if idSequenceStr != "*" {
		sequence, err := strconv.Atoi(idSequenceStr)
		if err != nil {
			return -1, fmt.Errorf("entryIDSequence is not a number, it's %s, err: %v", idSequenceStr, err)
		}
		return sequence, nil
	}

	// Handle wildcard sequence based on stream state
	if lastStreamEntry == nil {
		if idTimestamp == 0 {
			return 1, nil
		}
		return 0, nil
	}

	if lastStreamEntry.idTimestamp == uint64(idTimestamp) {
		return int(lastStreamEntry.idSequence) + 1, nil
	}

	return 0, nil
}

func validateEntryID(idTimestamp, idSequence int, lastStreamEntry *StreamEntry) error {
	// Check if ID is valid (greater than 0-0)
	if idTimestamp < 0 || (idTimestamp == 0 && idSequence < 1) {
		return fmt.Errorf("The ID specified in XADD must be greater than 0-0")
	}

	// If there's no last entry, no further validation needed
	if lastStreamEntry == nil {
		return nil
	}

	// Check if new ID is smaller than the stream's last entry
	if uint64(idTimestamp) < lastStreamEntry.idTimestamp {
		return fmt.Errorf("The ID specified in XADD is equal or smaller than the target stream top item")
	}

	// Check if timestamp is equal but sequence is smaller or equal
	if uint64(idTimestamp) == lastStreamEntry.idTimestamp && uint64(idSequence) <= lastStreamEntry.idSequence {
		return fmt.Errorf("The ID specified in XADD is equal or smaller than the target stream top item")
	}

	return nil
}

// Get gets a value for a key, considering expiration
func (s *Store) Get(key string) *KeyValue {
	// s.mu.RLock()
	// defer s.mu.RUnlock()

	valObj, ok := s.data[key]

	if !ok {
		return nil
	}
	// Check if key has expired
	if valObj.ExpireTime != "-1" {
		nowMS := utils.MillisecondsSince1970(time.Now())
		if valObj.ExpireTime < nowMS {
			return nil
		}
	}

	return &valObj
}

// Keys returns all keys matching a pattern
func (s *Store) Keys(pattern string) []string {
	// s.mu.RLock()
	// defer s.mu.RUnlock()

	requestedKeys := make([]string, 0)

	if pattern == "*" {
		for key := range s.data {
			requestedKeys = append(requestedKeys, key)
		}
	}
	// Additional pattern matching could be implemented here

	return requestedKeys
}

// LoadFromRDB loads data from a parsed RDB file
func (s *Store) LoadFromRDB(parsedRDBData *ParsedRDBData) {
	// s.mu.Lock()
	// defer s.mu.Unlock()

	// Clear existing data
	s.data = make(map[string]KeyValue)

	// Load data from RDB
	cacheFromRDB := parsedRDBData.DbIndexToDB[0]
	keyToExpireTime := parsedRDBData.DbIndexToKeyExpireTimeMap[0]

	for key, val := range cacheFromRDB {
		valType := getDataType(val)
		if _, exists := keyToExpireTime[key]; !exists {
			s.data[key] = KeyValue{ValueType: valType, Value: val, ExpireTime: "-1"}
			continue
		}

		expireTimeStr := keyToExpireTime[key]
		if expireTimeStr[len(expireTimeStr)-2:] == "ms" {
			expireTimeStr = expireTimeStr[:len(expireTimeStr)-2]
			expireTime := utils.UnixMsTimestampToTime(expireTimeStr)
			s.data[key] = KeyValue{ValueType: valType, Value: val, ExpireTime: utils.MillisecondsSince1970(expireTime)}
		} else if expireTimeStr[len(expireTimeStr)-1:] == "s" {
			expireTimeStr = expireTimeStr[:len(expireTimeStr)-1]
			expireTime := utils.UnixSecTimestampToTime(expireTimeStr)
			s.data[key] = KeyValue{ValueType: valType, Value: val, ExpireTime: utils.MillisecondsSince1970(expireTime)}
		}
	}
}

func (s *Store) PrintAll() {
	for key, val := range s.data {
		fmt.Printf("key: %s, val: %v\n", key, val)
	}
}

// Length returns number of unique entries
func (s *Store) Length() int {
	return len(s.data)
}

func getDataType(val interface{}) DataType {
	switch val.(type) {
	case string:
		return StringType
	case int:
		return IntegerType
	case *RedisStream:
		return StreamType
	default:
		return UnknownType
	}
}
