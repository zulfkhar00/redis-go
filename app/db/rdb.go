package db

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/protocol"
)

type ParsedRDBData struct {
	DbIndexToDB               map[uint64]map[string]string
	DbIndexToKeyExpireTimeMap map[uint64]map[string]string
	MetadataData              map[string]string
}

// RDBParser parses Redis RDB files
type RDBParser struct {
	dir        string
	dbfilename string
}

// NewRDBParser creates a new RDB parser
func NewRDBParser(dir, dbfilename string) *RDBParser {
	return &RDBParser{
		dir:        dir,
		dbfilename: dbfilename,
	}
}

// ReadRDBFile reads and parses an RDB file
func (r *RDBParser) ReadRDBFile() (*ParsedRDBData, error) {
	if r.dir == "" || r.dbfilename == "" {
		return &ParsedRDBData{
			DbIndexToDB:               make(map[uint64]map[string]string),
			DbIndexToKeyExpireTimeMap: make(map[uint64]map[string]string),
			MetadataData:              make(map[string]string),
		}, nil
	}

	filepath := r.dir
	if filepath[len(filepath)-1] != '/' {
		filepath += "/"
	}
	filepath += r.dbfilename

	// Check if file exists
	_, err := os.Stat(filepath)
	if os.IsNotExist(err) {
		return &ParsedRDBData{
			DbIndexToDB:               make(map[uint64]map[string]string),
			DbIndexToKeyExpireTimeMap: make(map[uint64]map[string]string),
			MetadataData:              make(map[string]string),
		}, nil
	}

	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Parse the RDB file content
	return r.parseRDBData(file)
}

// parseRDBData parses the RDB file
func (r *RDBParser) parseRDBData(file *os.File) (*ParsedRDBData, error) {
	err := r.parseHeaderSection(file)
	if err != nil {
		return nil, err
	}

	metadataData, err := r.parseMetadataSection(file)
	if err != nil {
		return nil, err
	}

	dbIndexToDB, dbIndexToKeyExpireTimeMap, err := r.parseDBSection(file)
	if err != nil {
		return nil, err
	}

	return &ParsedRDBData{
		DbIndexToDB:               dbIndexToDB,
		DbIndexToKeyExpireTimeMap: dbIndexToKeyExpireTimeMap,
		MetadataData:              metadataData,
	}, nil
}

// parseHeaderSection parses the RDB file header
func (r *RDBParser) parseHeaderSection(file *os.File) error {
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

// parseMetadataSection parses the metadata section of the RDB file
func (r *RDBParser) parseMetadataSection(file *os.File) (map[string]string, error) {
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
		metadataKey := r.decodeStringEncoding(file)
		metadataVal := r.decodeStringEncoding(file)
		metadata[metadataKey] = metadataVal
	}

	return metadata, nil
}

// parseDBSection parses the database section of the RDB file
func (r *RDBParser) parseDBSection(file *os.File) (map[uint64]map[string]string, map[uint64]map[string]string, error) {
	dbIndexToDB := make(map[uint64]map[string]string)
	dbIndexToKeyExpireTimeMap := make(map[uint64]map[string]string)

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
		dbIndex, err := r.decodeSizeEncoding(file)
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

		hashTableSize, err := r.decodeSizeEncoding(file)
		if err != nil {
			return nil, nil, fmt.Errorf("Error reading Database subsection hash table size: %v", err)
		}
		_, err = r.decodeSizeEncoding(file) // number of expiry keys
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
				key = r.decodeStringEncoding(file)
				val = r.decodeStringEncoding(file)
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

	// Read 8-byte CRC64 checksum
	checksum := make([]byte, 8)
	_, err := file.Read(checksum)
	if err != nil {
		return nil, nil, err
	}

	return dbIndexToDB, dbIndexToKeyExpireTimeMap, nil
}

// decodeStringEncoding decodes a string from the RDB file
func (r *RDBParser) decodeStringEncoding(file *os.File) string {
	stringSize, err := r.decodeSizeEncoding(file)
	if err != nil {
		switch err.(type) {
		case protocol.EightBitStringInteger:
			secondByte := make([]byte, 1)
			_, err = file.Read(secondByte)
			if err != nil {
				return "0"
			}
			return strconv.Itoa(int(secondByte[0]))
		case *protocol.SixteenBitStringInteger:
			nextTwoBytes := make([]byte, 2)
			_, err = file.Read(nextTwoBytes)
			if err != nil {
				return "0"
			}
			return strconv.FormatUint(binary.LittleEndian.Uint64(nextTwoBytes), 10)
		case *protocol.ThirtyTwoBitStringInteger:
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

// decodeSizeEncoding decodes a size encoding from the RDB file
func (r *RDBParser) decodeSizeEncoding(file *os.File) (uint64, error) {
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
			return 0, protocol.EightBitStringInteger{}
		case 0xC1:
			// 16-bit integer string encoding (little-endian)
			return 0, protocol.SixteenBitStringInteger{}
		case 0xC2:
			// 32-bit integer string encoding (little-endian)
			return 0, protocol.ThirtyTwoBitStringInteger{}
		default:
			// 0xC3 string encoding type means that the string is compressed with the LZF algorithm
			// skipping this case
			value = 0
		}

		return value, nil
	}

	return 0, fmt.Errorf("cannot understand size encoding")
}

func (r *RDBParser) OpenRDBFile() ([]byte, error) {
	emptyRDB, err := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
	if err != nil {
		return nil, err
	}

	return emptyRDB, nil
}
