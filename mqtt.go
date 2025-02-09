package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
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
	CONNECT_PACKET_ID     = 0b0001
	CONNACK_PACKET_ID     = 0b0010
	PUBLISH_PACKET_ID     = 0b0011
	PUBACK_PACKET_ID      = 0b0100
	PUBREC_PACKET_ID      = 0b0101
	PUBREL_PACKET_ID      = 0b0110
	PUBCOMP_PACKET_ID     = 0b0111
	SUBSCRIBE_PACKET_ID   = 0b1000
	SUBACK_PACKET_ID      = 0b1001
	UNSUBSCRIBE_PACKET_ID = 0b1010
	UNSUBACK_PACKET_ID    = 0b1011
	PINGREQ_PACKET_ID     = 0b1100
	PINGRESP_PACKET_ID    = 0b1101
	DISCONNECT_PACKET_ID  = 0b1110
	PORT                  = "1234"
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
	fixedHeader    [2]byte
	variableHeader []byte
	payload        []byte
	topic          string
}

type TypedSyncMap[K comparable, V any] struct {
	m sync.Map
}

var subscriptionMap TypedSyncMap[string, []string]
var writeWaitGroup sync.WaitGroup

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

func handlePayload(receivedData *[]byte, dataLength *int, client *Client) (err error) {
	// 10 | 1F <- Fixed header (1. packet type 2. remaining packet length)
	// 00 04 | 4D 51 54 54 | 05 | C2 | 00 3C <- Variable header (Order: 1. Protocol name length; 2. Protocol name; 3. Protocol level; 4. Connect flags; 5. Keep alive)
	if len(*receivedData) < 10 {
		err = errors.New("Packet length invalid.")
		return err
	}

	packetTypeBits := (*receivedData)[0] >> 4

	// CONNECT
	if packetTypeBits == CONNECT_PACKET_ID {
		protocolNameLength := int(binary.BigEndian.Uint16((*receivedData)[2:4]))
		if protocolNameLength != 4 {
			err = errors.New(fmt.Sprintf("Unsuported protocol length. Ending connection. Received data: % X\n", *receivedData))
			return err
		}

		protocolName := string((*receivedData)[4:8])
		if protocolName != "MQTT" {
			err = errors.New(fmt.Sprintf("Unsuported protocol name. Ending connection. Received data: % X\n", *receivedData))
			return err
		}

		return procConn(receivedData, dataLength, client)
	}
	// DISCONNECT
	if packetTypeBits == DISCONNECT_PACKET_ID {
		procDisconnect(receivedData, dataLength, client)
	}

	// PUBLISH
	if packetTypeBits == PUBLISH_PACKET_ID {
		procPublish(receivedData, dataLength, client)
	}

	// SUBSCRIBE
	if packetTypeBits == SUBSCRIBE_PACKET_ID {
		procSubscription(receivedData, dataLength, client)
	}

	// UNSUBSCRIBE
	if packetTypeBits == UNSUBACK_PACKET_ID {
		procUnsub(receivedData, dataLength, client)
	}

	// PINGREQ
	if packetTypeBits == PINGREQ_PACKET_ID {
		procPingreq(receivedData, dataLength, client)
	}

	err = errors.New(fmt.Sprintf("Error: could not interpret packet type. Received data: %s\n", *receivedData))
	return err
}

func connOutbbox(client *Client) {
	for {
		select {
		case <-client.ctx.Done():
			writeWaitGroup.Wait()
			log.Printf("Ended connection outbox for clientId: %s.\n", client.id)
			return
		default:
			select {
			case packet := <-client.outboxChan:
				writeWaitGroup.Add(1)

				byteFormatPacket, err := packetStructToByte(&packet)
				if err != nil {
					log.Printf("Error while converting Packet type struct variable to byte data: %s.\n", err)
					writeWaitGroup.Done()
					continue
				}

				writeLength, err := (*client.conn).Write(byteFormatPacket)
				if err != nil {
					log.Printf("Error while writing packet: %s.\n", err)
					writeWaitGroup.Done()
					continue
				}

				if writeLength != len(byteFormatPacket) {
					log.Printf("Error: written byte differ in size to source byte data. Sent: %d Data length: %d.\n", writeLength, len(byteFormatPacket))
					writeWaitGroup.Done()
					continue
				}

				log.Println("Message successfully written.")

				writeWaitGroup.Done()
			default:
				continue
			}
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
			var receivedData []byte
			dataLength, err := (*client.conn).Read(receivedData[:])
			if err != nil {
				log.Printf("Error: problems while handling conn read op: %s\n", err)
				client.cancel()
			}
			err = handlePayload(&receivedData, &dataLength, client)
			if err != nil {
				log.Printf("%s", err)
				continue
			}
		}
	}
}

func procPublish(receivedData *[]byte, dataLength *int, client *Client) {
	var payload Packet
	err := json.Unmarshal(bytes.TrimRight((*receivedData)[:*dataLength], "\x00"), &payload)
	if err != nil {
		log.Printf("Error: received data does not comply with expected format: %s\n", err)
		(*client.conn).Close()
	}
}

func procConn(receivedData *[]byte, dataLength *int, client *Client) error {
	// Exemplo:
	// 10 | 1F <- Fixed header (1. packet type 2. remaining packet length)
	// 00 04 | 4D 51 54 54 | 05 | C2 | 00 3C <- Variable header (Order: 1. Protocol name length; 2. Protocol name; 3. Protocol level; 4. Connect flags; 5. Keep alive)
	// Connect flags order: bit7 = username; bit6 = password; bit5 = will retain; bit4-3 = will qos; bit2 = has will; bit1 = clean session; bit0 = reserved.
	// 00 0B | 63 6C 69 65 6E 74 2D 30 30 31 <- 1. size of client id 2. client id
	// 00 08 | 75 73 65 72 6E 61 6D 65 <- 1. username length 2. username
	// 00 08 | 70 61 73 73 77 6F 72 64 <- 1. password length 2. password
	return nil
	// Ativar ou não mecanismo de keep alive
}

func procDisconnect(receivedData *[]byte, dataLength *int, client *Client) {
}

func procPingreq(receivedData *[]byte, dataLength *int, client *Client) {
}

func procSubscription(receivedData *[]byte, dataLength *int, client *Client) {
}

func procUnsub(receivedData *[]byte, dataLength *int, client *Client) {
}

func keepaliveTracker(client *Client) {
	for {
		select {
		case <-client.ctx.Done():
			log.Printf("Ended keep alive tracker for clientId: %s.\n", client.id)
			return
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

func createPingespPacket(client *Client) (packet Packet, err error) {
	return packet, nil
}

func createConnackPakcet(client *Client) (packet Packet, err error) {
	return packet, nil
}

func packetStructToByte(packet *Packet) (byteData []byte, err error) {
	return byteData, nil
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
	defer ln.Close()

	log.Printf("Listening to connections on port: %s\n", PORT)

	go func() {
		<-shutdownChan
		log.Println("Shut down signal received...")
		cancel()
		ln.Close()
		writeWaitGroup.Wait()
		log.Println("All write operations finished. Shutting down...")
		os.Exit(0)
	}()

	for {
		select {
		case <-appCtx.Done():
			continue
		default:
			conn, err := ln.Accept()
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Err.Error() == "use of closed network connection" {
					log.Println("Listener closed...")
					continue
				}
				log.Printf("Error: could not accept connection: %s\n", err)
				continue
			}
			log.Println("Accepted connection.")
			go handleConnection(&conn, &appCtx)
		}
	}
}
