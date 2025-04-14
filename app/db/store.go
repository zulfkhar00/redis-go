package db

import (
	"fmt"
	"math"
	"sort"
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

func (s *RedisStream) GetNewerEntries(minEntryID string) []StreamEntry {
	entries := make([]StreamEntry, 0)
	if minEntryID == "$" {
		s.entries.ForEach(func(node art.Node) (cont bool) {
			entry, ok := node.Value().(StreamEntry)
			if !ok {
				fmt.Printf("entry is not StreamEntry, it is %v\n", node.Value())
				return true
			}
			entries = append(entries, entry)
			return true
		})
		return entries
	}

	s.entries.ForEach(func(node art.Node) (cont bool) {
		nodeKey := string(node.Key())
		if nodeKey > minEntryID {
			entry, ok := node.Value().(StreamEntry)
			if !ok {
				fmt.Printf("entry is not StreamEntry, it is %v\n", node.Value())
				return true
			}
			entries = append(entries, entry)
		}
		return true
	})

	return entries
}

type StreamEntry struct {
	idTimestamp       uint64
	idSequence        uint64
	fields            map[string]string
	size              int64
	creationTimestamp uint64
}

func (s StreamEntry) GetID() string {
	return fmt.Sprintf("%d-%d", s.idTimestamp, s.idSequence)
}

func (s StreamEntry) GetFields() map[string]string {
	return s.fields
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

func (val *KeyValue) ToInt64() int64 {
	valInt, ok := val.Value.(int64)
	if !ok {
		return -1
	}
	return valInt
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

func (s *Store) SetStream(key, entryID string, fields map[string]string) (*RedisStream, string, error) {
	var idTimestampStr, idSequenceStr string
	if entryID == "*" {
		idTimestampStr = fmt.Sprintf("%d", time.Now().UnixMilli())
		idSequenceStr = "0"
	} else {
		entryIDParts := strings.Split(entryID, "-")
		idTimestampStr, idSequenceStr = entryIDParts[0], entryIDParts[1]
	}
	var finalStream *RedisStream

	entryKey := ""
	oldVal, exists := s.data[key]
	entryCreationTime := uint64(time.Now().UnixMilli())
	if !exists {
		idTimestamp, idSequence, err := checkAndConvertEntryIDs(idTimestampStr, idSequenceStr, nil)
		if err != nil {
			return nil, "", err
		}
		entryKey = fmt.Sprintf("%d-%d", idTimestamp, idSequence)

		entries := art.New()
		entry := StreamEntry{
			idTimestamp:       uint64(idTimestamp),
			idSequence:        uint64(idSequence),
			fields:            fields,
			size:              int64(len(fields)),
			creationTimestamp: entryCreationTime,
		}
		entries.Insert(art.Key(entryKey), art.Value(entry))
		finalStream = &RedisStream{entries: entries, size: 0, lastEntry: &entry}
		s.data[key] = KeyValue{ValueType: StreamType, Value: finalStream, ExpireTime: "-1"}
	} else {
		stream, ok := oldVal.Value.(*RedisStream)
		finalStream = stream
		if !ok {
			panic(fmt.Errorf("[SetStream] s.data[%q] is not a *RedisStream, it is %s", key, oldVal.ValueType.ToString()))
		}

		idTimestamp, idSequence, err := checkAndConvertEntryIDs(idTimestampStr, idSequenceStr, stream.lastEntry)
		if err != nil {
			return nil, "", err
		}
		entryKey = fmt.Sprintf("%d-%d", idTimestamp, idSequence)

		entry := StreamEntry{
			idTimestamp:       uint64(idTimestamp),
			idSequence:        uint64(idSequence),
			fields:            fields,
			size:              int64(len(fields)),
			creationTimestamp: entryCreationTime,
		}
		stream.entries.Insert(art.Key(entryKey), art.Value(entry))
		stream.lastEntry = &entry
	}

	return finalStream, entryKey, nil
}

func (s *Store) GetRangeStreamEntries(key, startEntryID, endEntryID string) ([]StreamEntry, error) {
	idTimestampStart, idSequenceStart, err := parseEntryID(startEntryID)
	if err != nil {
		return nil, err
	}
	idTimestampEnd, idSequenceEnd, err := parseEntryID(endEntryID)
	if err != nil {
		return nil, err
	}

	streamVal := s.Get(key)
	if streamVal == nil || streamVal.ValueType != StreamType {
		return []StreamEntry{}, nil
	}

	stream, ok := streamVal.Value.(*RedisStream)
	if !ok {
		panic(fmt.Errorf("[GetRangeStreamEntries] s.Get(%q) is not a *RedisStream, it is %s", key, streamVal.ValueType.ToString()))
	}
	entries := RadixTreeRangeQuery(stream.entries, int64(idTimestampStart), int64(idSequenceStart), int64(idTimestampEnd), int64(idSequenceEnd))
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].creationTimestamp < entries[j].creationTimestamp
	})

	return entries, nil
}

func (s *Store) GetNewerStreamEntries(key, minEntryID string) ([]StreamEntry, error) {
	if minEntryID == "$" {
		return []StreamEntry{}, nil
	}
	minIDTimestamp, minIDSequence, err := parseEntryID(minEntryID)
	if err != nil {
		return nil, err
	}

	streamVal := s.Get(key)
	if streamVal == nil || streamVal.ValueType != StreamType {
		return []StreamEntry{}, nil
	}

	stream, ok := streamVal.Value.(*RedisStream)
	if !ok {
		panic(fmt.Errorf("[GetNewerStreamEntries] s.Get(%q) is not a *RedisStream, it is %s", key, streamVal.ValueType.ToString()))
	}
	entries := RadixTreeQueryKeysGreaterThan(stream.entries, int64(minIDTimestamp), int64(minIDSequence))
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].creationTimestamp < entries[j].creationTimestamp
	})

	return entries, nil
}

func (s *Store) GetLatestStreamEntry(key string) *StreamEntry {
	streamVal := s.Get(key)
	if streamVal == nil || streamVal.ValueType != StreamType {
		return nil
	}
	stream, ok := streamVal.Value.(*RedisStream)
	if !ok {
		panic(fmt.Errorf("[GetLatestStreamEntry] s.Get(%q) is not a *RedisStream, it is %s", key, streamVal.ValueType.ToString()))
	}
	return stream.lastEntry
}

func parseEntryID(entryID string) (idTimestamp, idSequence int, err error) {
	if entryID == "-" {
		return 0, 0, nil
	}
	if entryID == "+" {
		return math.MaxInt, math.MaxInt, nil
	}
	parts := strings.Split(entryID, "-")
	if len(parts) == 0 {
		idTimestamp, err := strconv.Atoi(parts[0])
		if err != nil {
			return -1, -1, fmt.Errorf("entryID is not a number: %s", parts[0])
		}
		return idTimestamp, 0, nil
	}

	idTimestamp, err = strconv.Atoi(parts[0])
	if err != nil {
		return -1, -1, fmt.Errorf("entryID timestamp is not a number: %s", parts[0])
	}
	idSequence, err = strconv.Atoi(parts[1])
	if err != nil {
		return -1, -1, fmt.Errorf("entryID sequence is not a number: %s", parts[0])
	}

	return idTimestamp, idSequence, nil
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
	switch v := val.(type) {
	case string:
		if _, err := strconv.ParseInt(v, 10, 64); err == nil {
			return IntegerType
		}
		return StringType
	case int, int64:
		return IntegerType
	case *RedisStream:
		return StreamType
	default:
		return UnknownType
	}
}

func RadixTreeRangeQuery(tree art.Tree, startTS, startSeq, endTS, endSeq int64) []StreamEntry {
	startKey := fmt.Sprintf("%d-%d", startTS, startSeq)
	endKey := fmt.Sprintf("%d-%d", endTS, endSeq)

	var prefix string
	if startTS == endTS {
		prefix = fmt.Sprintf("%d-", startTS)
	} else {
		prefix = longestCommonPrefix(startKey, endKey)
	}

	res := make([]StreamEntry, 0)
	tree.ForEachPrefix(art.Key(prefix), func(node art.Node) bool {
		nodeKey := string(node.Key())
		if startKey <= nodeKey && nodeKey <= endKey {
			streamEntry, ok := node.Value().(StreamEntry)
			if !ok {
				fmt.Printf("RadixTreeRangeQuery err: stream entry is not StreamEntry, it is: %v\n", node.Value())
				return nodeKey <= endKey
			}
			res = append(res, streamEntry)
		}

		return nodeKey <= endKey
	})

	return res
}

func RadixTreeQueryKeysGreaterThan(tree art.Tree, minTS, minSeq int64) []StreamEntry {
	minKey := fmt.Sprintf("%d-%d", minTS, minSeq)
	res := make([]StreamEntry, 0)

	tree.ForEach(func(node art.Node) (cont bool) {
		nodeKey := string(node.Key())
		if nodeKey > minKey {
			streamEntry, ok := node.Value().(StreamEntry)
			if !ok {
				fmt.Printf("RadixTreeQueryKeysGreaterThan err: stream entry is not StreamEntry, it is: %v\n", node.Value())
				return true
			}
			res = append(res, streamEntry)
		}
		return true
	})

	return res
}

func longestCommonPrefix(s1, s2 string) string {
	minLen := len(s1)
	if len(s2) < minLen {
		minLen = len(s2)
	}

	for i := 0; i < minLen; i++ {
		if s1[i] != s2[i] {
			return s1[:i]
		}
	}

	return s1[:minLen]
}
