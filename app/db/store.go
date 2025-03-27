package db

import (
	"fmt"
	"sync"
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
	fields map[string]string
	size   int64
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
	mu   sync.RWMutex
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]KeyValue),
	}
}

func (s *Store) Set(key string, val interface{}, expireMS int) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if expiration time is provided
	valType := getDataType(val)
	if expireMS < 0 {
		s.data[key] = KeyValue{ValueType: valType, Value: val, ExpireTime: "-1"}
	} else {
		expireTime := time.Now().Add(time.Duration(expireMS) * time.Millisecond)
		s.data[key] = KeyValue{ValueType: valType, Value: val, ExpireTime: utils.MillisecondsSince1970(expireTime)}
	}

	return "OK"
}

func (s *Store) SetStream(key, entryKey string, fields map[string]string) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldVal, exists := s.data[key]
	if !exists {
		entries := art.New()
		entry := StreamEntry{fields: fields, size: int64(len(fields))}
		entries.Insert(art.Key(entryKey), art.Value(entry))

		stream := RedisStream{entries: entries, size: 0, lastEntry: nil}
		s.data[key] = KeyValue{ValueType: StreamType, Value: stream, ExpireTime: "-1"}
	} else {
		stream, ok := oldVal.Value.(*RedisStream)
		if !ok {
			panic(fmt.Errorf("[SetStream] s.data[%q] is not a *RedisStream, it is %s", key, oldVal.ValueType.ToString()))
		}
		entry := StreamEntry{fields: fields, size: int64(len(fields))}
		stream.entries.Insert(art.Key(entryKey), art.Value(entry))
	}

	return entryKey
}

// Get gets a value for a key, considering expiration
func (s *Store) Get(key string) *KeyValue {
	s.mu.RLock()
	defer s.mu.RUnlock()

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
	s.mu.RLock()
	defer s.mu.RUnlock()

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
	s.mu.Lock()
	defer s.mu.Unlock()

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
