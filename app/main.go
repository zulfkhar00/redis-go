package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
)

var _ = net.Listen
var _ = os.Exit

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()
	go func() {
		connection, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		defer connection.Close()

		buf := make([]byte, 1024)
		for {
			_, err := connection.Read(buf)
			if errors.Is(err, io.EOF) {
				break
			}

			_, err = connection.Write([]byte("+PONG\r\n"))
			if err != err {
				return
			}
		}
	}()
}
