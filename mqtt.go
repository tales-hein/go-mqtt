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
	ticker             *time.Ticker
	keepAliveCountdown int
}

type Flags struct {
	hasUsername  bool
	hasPassword  bool
	cleanSession bool
	reserved     bool
	hasWill      bool
	willRetain   bool
	willQOS      int
	keepAlive    int
}

type Client struct {
	id         string
	ctx        context.Context
	cancel     context.CancelFunc
	outboxChan chan Packet
	inboxChan  chan Packet
	conn       *net.Conn
	connTicker *ConnTicker
	flags      *Flags
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

func handlePayload(data *[]byte, dataLength *int, client *Client) (err error) {
	if len(*data) < 2 {
		err = errors.New("Packet length invalid for any MQTT packet.")
		log.Printf("Received data: % X", *data)
		return err
	}

	packetTypeBits := (*data)[0] >> 4

	// CONNECT
	if packetTypeBits == CONNECT_PACKET_ID {
		protocolNameLength := int(binary.BigEndian.Uint16((*data)[2:4]))
		if protocolNameLength != 4 {
			err = errors.New(fmt.Sprintf("Unsuported protocol length. Ending connection. Received data: % X\n", *data))
			return err
		}

		protocolName := string((*data)[4:8])
		if protocolName != "MQTT" {
			err = errors.New(fmt.Sprintf("Unsuported protocol name. Ending connection. Received data: % X\n", *data))
			return err
		}

		return procConn(data, client)
	}

	// DISCONNECT
	if packetTypeBits == DISCONNECT_PACKET_ID {
		procDisconnect(data, dataLength, client)
	}

	// PUBLISH
	if packetTypeBits == PUBLISH_PACKET_ID {
		procPublish(data, dataLength, client)
	}

	// SUBSCRIBE
	if packetTypeBits == SUBSCRIBE_PACKET_ID {
		procSubscription(data, dataLength, client)
	}

	// UNSUBSCRIBE
	if packetTypeBits == UNSUBACK_PACKET_ID {
		procUnsub(data, dataLength, client)
	}

	// PINGREQ
	if packetTypeBits == PINGREQ_PACKET_ID {
		procPingreq(data, dataLength, client)
	}

	err = errors.New(fmt.Sprintf("Error: could not interpret packet type. Received data: %s\n", *data))
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
			receivedData := make([]byte, 128)
			dataLength, err := (*client.conn).Read(receivedData)
			if err != nil {
				log.Printf("Error: problems while handling conn read op: %s\n", err)
				client.cancel()
			}
			if dataLength > 0 {
				trimmedData := make([]byte, dataLength)
				copy(trimmedData, receivedData[:dataLength])
				err = handlePayload(&trimmedData, &dataLength, client)
				if err != nil {
					log.Printf("%s", err)
					client.cancel()
				}
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

func procConn(receivedData *[]byte, client *Client) error {
	log.Printf("Received CONNECT packet data: % X\n", *receivedData)

	connectFlags := (*receivedData)[9]
	log.Printf("Connect Flags byte: %08b\n", connectFlags)

	(*client.flags) = Flags{
		hasUsername:  (connectFlags&(1<<7) != 0),
		hasPassword:  (connectFlags&(1<<6) != 0),
		willRetain:   (connectFlags&(1<<5) != 0),
		hasWill:      (connectFlags&(1<<2) != 0),
		cleanSession: (connectFlags&(1<<1) != 0),
		reserved:     (connectFlags&(1<<0) != 0),
		willQOS:      int((connectFlags & (1<<3 | 1<<4)) >> 3),
		keepAlive:    int(binary.BigEndian.Uint16((*receivedData)[10:12])),
	}

	clientIdLenght := int(binary.BigEndian.Uint16((*receivedData)[12:14]))
	client.id = string((*receivedData)[14 : 14+clientIdLenght])

	log.Println("Extracted Connect Flags:")
	log.Printf("  hasUsername:  %v\n", (*client.flags).hasUsername)
	log.Printf("  hasPassword:  %v\n", (*client.flags).hasPassword)
	log.Printf("  willRetain:   %v\n", (*client.flags).willRetain)
	log.Printf("  hasWill:      %v\n", (*client.flags).hasWill)
	log.Printf("  cleanSession: %v\n", (*client.flags).cleanSession)
	log.Printf("  reserved:     %v\n", (*client.flags).reserved)
	log.Printf("  willQOS:      %d\n", (*client.flags).willQOS)
	log.Printf("  keepAlive:    %d seconds\n", (*client.flags).keepAlive)
	log.Printf("Keep Alive value: %d seconds\n", (*client.flags).keepAlive)

	if (*client.flags).keepAlive > 0 {
		(*client.connTicker).keepAliveCountdown = (*client.flags).keepAlive
		(*client.connTicker).ticker = time.NewTicker(1 * time.Second)
		go keepaliveTracker(client)
		log.Println("Keep alive countdown set.")
	}

	// write connack
	return nil
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
					client.connTicker.keepAliveCountdown--
					if client.connTicker.keepAliveCountdown == 0 {
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
		id:         "unknown_" + strconv.Itoa(generateRandomId()),
		conn:       conn,
		ctx:        clientCtx,
		cancel:     cancel,
		outboxChan: make(chan Packet, 100),
		inboxChan:  make(chan Packet, 100),
		connTicker: &ConnTicker{},
		flags:      &Flags{},
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
