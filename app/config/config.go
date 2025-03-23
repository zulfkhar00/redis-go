package config

import "flag"

const (
	DefaultPort         = 6379
	DefaultMasterReplID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
)

// Config holds the server configuration
type Config struct {
	Dir        string
	DbFileName string
	Port       int

	// Replication info
	MasterHostAndPort string
	Role              string
	MasterReplID      string
	MasterReplOffset  int
}

var RedisInfo struct {
	ReplicationInfo struct {
		Role             string
		MasterReplID     string
		MasterReplOffset int
	}
}

func ParseFlags() *Config {
	cfg := &Config{}

	flag.StringVar(&cfg.Dir, "dir", "", "RDB directory path")
	flag.StringVar(&cfg.DbFileName, "dbfilename", "", "RDB file name")
	flag.IntVar(&cfg.Port, "port", DefaultPort, "Port number")
	flag.StringVar(&cfg.MasterHostAndPort, "replicaof", "", "Address of master/parent server")

	flag.Parse()

	return cfg
}

func ConfigureInfo(cfg *Config) {
	role := "master"
	masterReplID := DefaultMasterReplID
	masterOffset := 0

	if cfg.MasterHostAndPort != "" {
		role = "slave"
		masterReplID = ""
		masterOffset = -1
	}

	// Update the global redis info
	RedisInfo.ReplicationInfo.Role = role
	RedisInfo.ReplicationInfo.MasterReplID = masterReplID
	RedisInfo.ReplicationInfo.MasterReplOffset = masterOffset

	// Update the config
	cfg.Role = role
	cfg.MasterReplID = masterReplID
	cfg.MasterReplOffset = masterOffset
}
