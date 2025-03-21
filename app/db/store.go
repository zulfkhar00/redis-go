package db

import (
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

// KeyValue represents a value with an optional expiration time
type KeyValue struct {
	Value      string
	ExpireTime string // "-1" if no expiration
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

func (s *Store) Set(key, val string, expireMS int) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if expiration time is provided
	if expireMS < 0 {
		s.data[key] = KeyValue{Value: val, ExpireTime: "-1"}
	} else {
		expireTime := time.Now().Add(time.Duration(expireMS) * time.Millisecond)
		s.data[key] = KeyValue{Value: val, ExpireTime: utils.MillisecondsSince1970(expireTime)}
	}

	return "OK"
}

// Get gets a value for a key, considering expiration
func (s *Store) Get(key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	valObj, ok := s.data[key]
	if !ok {
		return ""
	}

	// Check if key has expired
	if valObj.ExpireTime != "-1" {
		nowMS := utils.MillisecondsSince1970(time.Now())
		if valObj.ExpireTime < nowMS {
			return ""
		}
	}

	return valObj.Value
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
		if _, exists := keyToExpireTime[key]; !exists {
			s.data[key] = KeyValue{Value: val, ExpireTime: "-1"}
			continue
		}

		expireTimeStr := keyToExpireTime[key]
		if expireTimeStr[len(expireTimeStr)-2:] == "ms" {
			expireTimeStr = expireTimeStr[:len(expireTimeStr)-2]
			expireTime := utils.UnixMsTimestampToTime(expireTimeStr)
			s.data[key] = KeyValue{Value: val, ExpireTime: utils.MillisecondsSince1970(expireTime)}
		} else if expireTimeStr[len(expireTimeStr)-1:] == "s" {
			expireTimeStr = expireTimeStr[:len(expireTimeStr)-1]
			expireTime := utils.UnixSecTimestampToTime(expireTimeStr)
			s.data[key] = KeyValue{Value: val, ExpireTime: utils.MillisecondsSince1970(expireTime)}
		}
	}
}
