package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const (
	CONNECT_BYTE     = 0b0001
	CONNACK_BYTE     = 0b0010
	PUBLISH_BYTE     = 0b0011
	PUBACK_BYTE      = 0b0100
	PUBREC_BYTE      = 0b0101
	PUBREL_BYTE      = 0b0110
	PUBCOMP_BYTE     = 0b0111
	SUBSCRIBE_BYTE   = 0b1000
	SUBACK_BYTE      = 0b1001
	UNSUBSCRIBE_BYTE = 0b1010
	UNSUBACK_BYTE    = 0b1011
	PINGREQ_BYTE     = 0b1100
	PINGRESP_BYTE    = 0b1101
	DISCONNECT_BYTE  = 0b1110
	MAX_DATA_SIZE    = 512
	PORT             = "1234"
)

const (
	MqttConnTimeout = 60 * time.Second
	TcpConnTimeout  = 10 * time.Second
)

type ConnTicker struct {
	ticker            *time.Ticker
	keepAliveInterval int
}

type Client struct {
	id         string
	conn       *net.Conn
	ctx        context.Context
	cancel     context.CancelFunc
	outboxChan chan Packet
	inboxChan  chan Packet
	connTicker *ConnTicker
}

type Packet struct {
	header  byte
	topic   string
	payload [MAX_DATA_SIZE]byte
}

type TypedSyncMap[K comparable, V any] struct {
	m sync.Map
}

var subscriptionMap TypedSyncMap[string, []string]

func (t *TypedSyncMap[K, V]) Store(key K, value V) {
	t.m.Store(key, value)
}

func (t *TypedSyncMap[K, V]) Load(key K) (V, bool) {
	value, ok := t.m.Load(key)
	if !ok {
		var zeroValue V
		return zeroValue, false
	}
	return value.(V), true
}

func (t *TypedSyncMap[K, V]) Delete(key K) {
	t.m.Delete(key)
}

func (t *TypedSyncMap[K, V]) Range(f func(K, V) bool) {
	t.m.Range(func(key, value interface{}) bool {
		return f(key.(K), value.(V))
	})
}

func generateRandomId() int {
	return rand.Intn(10000)
}

func handlePayload(receivedData *[MAX_DATA_SIZE]byte, dataLength *int, client *Client) {
	firstFourHeaderBits := receivedData[0] >> 4

	// CONNECT
	if firstFourHeaderBits == CONNECT_BYTE {
		procConn(receivedData, dataLength, client)
	}
	// DISCONNECT
	if firstFourHeaderBits == DISCONNECT_BYTE {
		procDisconnect(receivedData, dataLength, client)
	}

	// PUBLISH
	if firstFourHeaderBits == PUBLISH_BYTE {
		procPublish(receivedData, dataLength, client)
	}

	// SUBSCRIBE
	if firstFourHeaderBits == SUBSCRIBE_BYTE {
		procSubscription(receivedData, dataLength, client)
	}

	// UNSUBSCRIBE
	if firstFourHeaderBits == UNSUBACK_BYTE {
		procUnsub(receivedData, dataLength, client)
	}

	// PINGREQ
	if firstFourHeaderBits == PINGRESP_BYTE {
		procPingreq(receivedData, dataLength, client)
	}

	log.Printf("Error: could not interpret packet type. Received data: %s\n", *receivedData)
	client.cancel()
}

func connOutbbox(client *Client) {
	for {
		select {
		case <-client.ctx.Done():
			log.Printf("Ended connection outbox for clientId: %s.\n", client.id)
			return
		default:

		}
	}
}

func connInbox(client *Client) {
	for {
		select {
		case <-client.ctx.Done():
			log.Printf("Ended connection inbox for clientId: %s.\n", client.id)
			return
		default:
			var receivedData [MAX_DATA_SIZE]byte
			dataLength, err := (*client.conn).Read(receivedData[:])
			if err != nil {
				log.Printf("Error: problems while handling conn read op: %s\n", err)
				client.cancel()
			}
			handlePayload(&receivedData, &dataLength, client)
		}
	}
}

func procPublish(receivedData *[MAX_DATA_SIZE]byte, dataLength *int, client *Client) {
	var payload Packet
	err := json.Unmarshal(bytes.TrimRight((*receivedData)[:*dataLength], "\x00"), &payload)
	if err != nil {
		log.Printf("Error: received data does not comply with expected format: %s\n", err)
		(*client.conn).Close()
	}
}

func procConn(receivedData *[MAX_DATA_SIZE]byte, dataLength *int, client *Client) {
	// Ativar ou não mecanismo de keep alive
}

func procDisconnect(receivedData *[MAX_DATA_SIZE]byte, dataLength *int, client *Client) {
}

func procPingreq(receivedData *[MAX_DATA_SIZE]byte, dataLength *int, client *Client) {
}

func procSubscription(receivedData *[MAX_DATA_SIZE]byte, dataLength *int, client *Client) {
}

func procUnsub(receivedData *[MAX_DATA_SIZE]byte, dataLength *int, client *Client) {
}

func keepaliveTracker(client *Client) {
	for {
		select {
		case <-client.ctx.Done():

		default:
			if client.connTicker != nil {
				select {
				case <-client.connTicker.ticker.C:
					client.connTicker.keepAliveInterval--
					if client.connTicker.keepAliveInterval == 0 {
						client.cancel()
					}
				}
			}
		}
	}
}

func createPingreqPacket(client *Client) (packet Packet) {
	return packet
}

func handleConnection(conn *net.Conn, appCtx *context.Context) {
	clientCtx, cancel := context.WithCancel(*appCtx)

	newClient := Client{
		id:         strconv.Itoa(generateRandomId()),
		conn:       conn,
		ctx:        clientCtx,
		cancel:     cancel,
		outboxChan: make(chan Packet, 100),
		inboxChan:  make(chan Packet, 100),
		connTicker: nil,
	}

	go connInbox(&newClient)
	go connOutbbox(&newClient)
}

func main() {
	appCtx, cancel := context.WithCancel(context.Background())
	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	subscriptionMap = TypedSyncMap[string, []string]{}

	ln, err := net.Listen("tcp", ":"+PORT)
	if err != nil {
		log.Fatalf("Error: could not listen to port %s: %s\n", PORT, err)
	}

	log.Printf("Listening to connections on port: %s\n", PORT)

	go func() {
		<-shutdownChan
		log.Println("Shutting down...")
		cancel()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Error: could not accept connection: %s\n", err)
		}
		log.Println("Accepted connection.")
		go handleConnection(&conn, &appCtx)
	}
}
