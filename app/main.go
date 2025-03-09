package main

import (
	"fmt"
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

	connection, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer connection.Close()

	for {
		buf := make([]byte, 1024)
		n, err := connection.Read(buf)
		fmt.Printf("bytes received: %d", n)
		if err != nil {
			return
		}
		if n != 0 {
			connection.Write([]byte("+PONG\r\n"))
		}
	}
}
