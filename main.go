package main

import (
	"fmt"
	"log"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/db"
	"github.com/codecrafters-io/redis-starter-go/app/server"
)

var _ = net.Listen
var _ = os.Exit

func main() {
	// parse flags from command-line
	cfg := config.ParseFlags()

	// configure replication info
	config.ConfigureInfo(cfg)

	// Initialize the database
	kvStore := db.NewStore()

	// Load RDB file if specified
	if cfg.DbFileName != "" && cfg.Dir != "" {
		rdbParser := db.NewRDBParser(cfg.Dir, cfg.DbFileName)
		parsedRDBData, err := rdbParser.ReadRDBFile()
		if err != nil {
			log.Printf("Error reading RDB file: %v", err)
		} else {
			kvStore.LoadFromRDB(parsedRDBData)
		}
	}

	// Start the server
	if cfg.Role == "master" {
		server := server.NewServer(cfg, kvStore)

		// Start the master server
		err := server.Start()
		if err != nil {
			fmt.Printf("Error starting master server: %v\n", err)
			os.Exit(1)
		}
	}

	if cfg.Role == "slave" {
		replica := server.NewReplica(cfg, kvStore)

		// If replica mode is enabled, connect to master
		err := replica.ConnectToMaster()
		if err != nil {
			fmt.Printf("Error connecting to master: %v\n", err)
			os.Exit(1)
		}

		// Start the replica server
		err = replica.Start()
		if err != nil {
			fmt.Printf("Error starting replica server: %v\n", err)
			os.Exit(1)
		}
	}
}
