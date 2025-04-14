package utils

import (
	"fmt"
	"log"
	"strconv"
	"time"
)

func MillisecondsSince1970(t time.Time) string {
	seconds := t.Unix()
	milliseconds := seconds*1000 + int64(t.Nanosecond())/1000000
	return fmt.Sprint(milliseconds)
}

func UnixMsTimestampToTime(unixTimestamp string) time.Time {
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

func UnixSecTimestampToTime(unixTimestamp string) time.Time {
	// Convert the string to an int64 (seconds)
	expireTimestampSec, err := strconv.ParseInt(unixTimestamp, 10, 64)
	if err != nil {
		log.Fatal("Error converting timestamp string:", err)
	}

	return time.Unix(expireTimestampSec, 0) // 0 nanoseconds
}

func ToInt64(val string) (int64, error) {
	valInt, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return -1, err
	}
	return valInt, nil
}
