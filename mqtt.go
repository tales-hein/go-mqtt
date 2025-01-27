package mqtt

import (
	"encoding/json"
	"log"
	"net"
	"time"
)

const (
	Port        = "1234"
	ConnTimeout = 60 * time.Second
)

type connection struct {
	conn_id int
	conn    *net.Conn
}

type topic struct {
	connections []connection
	name        string
}

type payload struct {
	topic   string
	message string
}

var outboxChannel chan payload
var inboxChannel chan []byte

func tcpConnWrite(message *[]byte, conn *net.Conn) {
	n, err := (*conn).Write(*message)
	if err != nil {
		log.Printf("Error: could not write message to connection.")
		return
	}

	if n < len(*message) {
		log.Printf("Error: The message was not fully written.")
	}
}

func tcpConnRead(message *[]byte, conn *net.Conn) {

}

func serverOutbbox() {

}

func connInbox(conn *net.Conn) {

}

func processSubscription(receivedData *[]byte, conn *net.Conn) {
	(*conn).SetDeadline(time.Now().Add(ConnTimeout))
}

func processPublish(receivedData *[]byte, conn *net.Conn) {
	(*conn).SetDeadline(time.Now().Add(ConnTimeout))
	var payload payload
	err := json.Unmarshal(*receivedData, &payload)
	if err != nil {
		log.Printf("Error: received data does not comply with expected format: %s\n", err)
		(*conn).Close()
	}

}

func handleConnection(conn *net.Conn) {
	(*conn).SetDeadline(time.Now().Add(ConnTimeout))
	go connInbox(conn)
}

func main() {
	ln, err := net.Listen("tcp", ":"+Port)
	if err != nil {
		log.Fatalf("Error: could not listen to port %s: %s\n", Port, err)
	}

	go serverOutbbox()

	log.Printf("Listening to connections on port :%s", Port)

	outboxChannel = make(chan payload)
	inboxChannel = make(chan []byte)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Error: could not accept connection: %s\n", err)
		}
		log.Printf("Accepted connection.")
		go handleConnection(&conn)
	}
}
