package main

import (
	"context"
	"encoding/binary"
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
	CONNECT_PACKET_ID     = byte(0x10)
	CONNACK_PACKET_ID     = byte(0x20)
	PUBLISH_PACKET_ID_0   = byte(0x30)
	PUBLISH_PACKET_ID_1   = byte(0x32)
	PUBLISH_PACKET_ID_2   = byte(0x34)
	PUBACK_PACKET_ID      = byte(0x40)
	PUBREC_PACKET_ID      = byte(0x50)
	PUBREL_PACKET_ID      = byte(0x62)
	PUBCOMP_PACKET_ID     = byte(0x70)
	SUBSCRIBE_PACKET_ID   = byte(0x82)
	SUBACK_PACKET_ID      = byte(0x90)
	UNSUBSCRIBE_PACKET_ID = byte(0xA2)
	UNSUBACK_PACKET_ID    = byte(0xB0)
	PINGREQ_PACKET_ID     = byte(0xC0)
	PINGRESP_PACKET_ID    = byte(0xD0)
	DISCONNECT_PACKET_ID  = byte(0xE0)

	CONNECTION_ACCEPTED_BYTE           = byte(0x0)
	UNACCEPTABLE_PROTOCOL_VERSION_BYTE = byte(0x01)

	PORT = "1234"
)

const (
	MqttConnTimeout = 60 * time.Second
	TcpConnTimeout  = 10 * time.Second
)

type ConnTicker struct {
	ticker             *time.Ticker
	keepAliveCountdown int
}

type Message struct {
	topic   string
	payload []byte
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
	id          string
	ctx         context.Context
	cancel      context.CancelFunc
	outboxChan  chan Packet
	inboxChan   chan Packet
	conn        *net.Conn
	connTicker  *ConnTicker
	flags       *Flags
	pubRelQueue *TypedSyncMap[uint16, *Message]
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

var subscriptionMap TypedSyncMap[string, []string]
var writeWaitGroup sync.WaitGroup

func generateRandomId() int {
	return rand.Intn(10000)
}

func handlePayload(data *[]byte, dataLength *int, client *Client) (err error) {
	if len(*data) < 2 {
		err = errors.New("Packet length invalid for any MQTT packet.")
		log.Printf("Received data: % X", *data)
		return err
	}

	packetTypeByte := (*data)[0]

	// CONNECT
	if packetTypeByte == CONNECT_PACKET_ID {
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

		protocolLevel := int((*data)[8])
		if protocolLevel != 4 {
			err = errors.New(fmt.Sprintf("Unsuported protocol level. Ending connection. Received data: % X\n", *data))
			return err
		}

		procConn(data, client)

		err = sendConnack(client, CONNECTION_ACCEPTED_BYTE)
		if err != nil {
			return err
		}

		return nil
	}

	// DISCONNECT
	if packetTypeByte == DISCONNECT_PACKET_ID {
		log.Println("Client requested disconnect.")
		// TODO LAST WILL
		client.cancel()
		return nil
	}

	// PUBLISH QoS 0
	if packetTypeByte == PUBLISH_PACKET_ID_0 {
		err = procPublish(data, client, 0)
		if err != nil {
			return err
		}
		return nil
	}

	// PUBLISH QoS 1
	if packetTypeByte == PUBLISH_PACKET_ID_1 {
		err = procPublish(data, client, 1)
		if err != nil {
			return err
		}
		return nil
	}

	// PUBLISH QoS 2
	if packetTypeByte == PUBLISH_PACKET_ID_2 {
		err = procPublish(data, client, 2)
		if err != nil {
			return err
		}
		return nil
	}

	// PUBREL
	if packetTypeByte == PUBREL_PACKET_ID {
		err = procPubRel(data, client)
		if err != nil {
			return err
		}
		return nil
	}

	// SUBSCRIBE
	if packetTypeByte == SUBSCRIBE_PACKET_ID {
		err = procSubscription(data, dataLength, client)
		if err != nil {
			return err
		}
		return nil
	}

	// UNSUBSCRIBE
	if packetTypeByte == UNSUBSCRIBE_PACKET_ID {
		err = procUnsub(data, dataLength, client)
		if err != nil {
			return err
		}
		return nil
	}

	// PINGREQ
	if packetTypeByte == PINGREQ_PACKET_ID {
		err = procPingreq(client)
		if err != nil {
			return err
		}
		return nil
	}

	err = errors.New(fmt.Sprintf("Error: could not interpret packet type. Received data: % X\n", *data))
	return err
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

func procPublish(receivedData *[]byte, client *Client, qosLevel int) (err error) {
	log.Printf("Received PUBLISH packet data: % X\n", *receivedData)

	dataLength := len(*receivedData)
	if dataLength < 4 {
		err = errors.New(fmt.Sprintf("Error: Packet length invalid for PUBLISH MQTT packet. Received data: % X\n", *receivedData))
		client.cancel()
		return err
	}

	topicLength := int(binary.BigEndian.Uint16((*receivedData)[2:4]))
	if 4+topicLength > dataLength {
		err = errors.New(fmt.Sprintln("Error: Invalid topic length in PUBLISH packet."))
		client.cancel()
		return err
	}

	topic := string((*receivedData)[4 : 4+topicLength])

	var payloadStart int
	var packetID uint16

	if qosLevel > 0 {
		if 4+topicLength+2 > dataLength {
			err = errors.New(fmt.Sprintln("Error: Missing packet identifier in PUBLISH packet."))
			return err
		}
		packetID = binary.BigEndian.Uint16((*receivedData)[4+topicLength : 6+topicLength])
		payloadStart = 6 + topicLength
	} else {
		payloadStart = 4 + topicLength
	}

	payload := (*receivedData)[payloadStart:dataLength]

	log.Printf("Received PUBLISH packet - Topic: %s, QoS: %d, Packet ID: %d, Payload: %s\n", topic, qosLevel, packetID, payload)

	switch qosLevel {
	case 0:
		queueMessageForDelivery(topic, payload)
	case 1:
		queueMessageForDelivery(topic, payload)
		sendPubAck(client, packetID)
	case 2:
		sendPubRec(client, packetID)
		client.pubRelQueue.Store(packetID, &Message{topic, payload})
	}

	return nil
}

func sendPubAck(client *Client, packetID uint16) (err error) {
	var packet [4]byte
	packet[0] = PUBACK_PACKET_ID
	packet[1] = 0x02
	binary.BigEndian.PutUint16(packet[2:], packetID)

	log.Printf("Sending PUBACK packet data: % X\n", packet)

	wrote, err := (*client.conn).Write(packet[:])
	if err != nil {
		err = errors.New(fmt.Sprintf("Error sending PUBACK: %s\n", err))
		client.cancel()
		return err
	}

	if wrote != len(packet) {
		err = errors.New(fmt.Sprintf("Error: PUBACK sent length mismatch. Sent: %d, Expected: %d\n", wrote, len(packet)))
		client.cancel()
		return err
	}

	log.Printf("PUBACK sent for Packet ID: %d\n", packetID)

	return nil
}

func procSubscription(receivedData *[]byte, dataLength *int, client *Client) (err error) {
	log.Printf("Received SUBSCRIBE packet data: % X\n", *receivedData)

	if *dataLength < 4 {
		err = errors.New(fmt.Sprintf("Error: Packet length invalid for SUBSCRIBE MQTT packet. Received data: % X\n", *receivedData))
		client.cancel()
		return err
	}

	remainingLength := int((*receivedData)[1])
	packetID := binary.BigEndian.Uint16((*receivedData)[2:4])
	currentTopicNameLength := int(binary.BigEndian.Uint16((*receivedData)[4:6]))

	var topicNames []string
	var requestedTopicsQOS []byte
	cursorPos := 6

	for cursorPos <= remainingLength {
		if len(topicNames) >= 1 {
			currentTopicNameLength = int(binary.BigEndian.Uint16((*receivedData)[cursorPos : cursorPos+2]))
			cursorPos += 2
		}
		topicNames = append(topicNames, string((*receivedData)[cursorPos:cursorPos+currentTopicNameLength]))
		cursorPos += currentTopicNameLength
		requestedTopicsQOS = append(requestedTopicsQOS, (*receivedData)[cursorPos])
		cursorPos++
	}

	log.Printf("Subscribing Client: %s. To: %v. With QoS: %v.", (*client).id, topicNames, requestedTopicsQOS)

	err = sendSubAck(client, packetID, requestedTopicsQOS)
	if err != nil {
		return err
	}

	return nil
}

func sendSubAck(client *Client, packetID uint16, requestedTopicsQoS []byte) (err error) {
	packetLength := len(requestedTopicsQoS) + 4
	packet := make([]byte, packetLength)

	packet[0] = SUBACK_PACKET_ID
	packet[1] = byte(len(requestedTopicsQoS) + 2)
	binary.BigEndian.PutUint16(packet[2:4], packetID)
	copy(packet[4:], requestedTopicsQoS)

	log.Printf("Sending SUBACK packet data: % X\n", packet)

	wrote, err := (*client.conn).Write(packet[:])
	if err != nil {
		err = errors.New(fmt.Sprintf("Error sending SUBACK: %s\n", err))
		client.cancel()
		return err
	}

	if wrote != len(packet) {
		err = errors.New(fmt.Sprintf("Error: SUBACK sent length mismatch. Sent: %d, Expected: %d\n", wrote, len(packet)))
		client.cancel()
		return err
	}

	log.Printf("SUBACK sent for Packet ID: %d\n", packetID)

	return nil
}

func procUnsub(receivedData *[]byte, dataLength *int, client *Client) (err error) {
	log.Printf("Received UNSUBSCRIBE packet data: % X\n", *receivedData)

	if *dataLength < 4 {
		err = errors.New(fmt.Sprintf("Error: Packet length invalid for UNSUBSCRIBE MQTT packet. Received data: % X\n", *receivedData))
		client.cancel()
		return err
	}

	remainingLength := int((*receivedData)[1])
	packetID := binary.BigEndian.Uint16((*receivedData)[2:4])
	currentTopicNameLength := int(binary.BigEndian.Uint16((*receivedData)[4:6]))

	var topicNames []string
	cursorPos := 6

	for cursorPos <= remainingLength {
		if len(topicNames) >= 1 {
			currentTopicNameLength = int(binary.BigEndian.Uint16((*receivedData)[cursorPos : cursorPos+2]))
			cursorPos += 2
		}
		topicNames = append(topicNames, string((*receivedData)[cursorPos:cursorPos+currentTopicNameLength]))
		cursorPos += currentTopicNameLength
	}

	log.Printf("Unsubscribing Client: %s. To: %v.", (*client).id, topicNames)

	err = sendUnsubAck(client, packetID)
	if err != nil {
		return err
	}

	return nil
}

func sendUnsubAck(client *Client, packetID uint16) (err error) {
	var packet [4]byte
	packet[0] = UNSUBACK_PACKET_ID
	packet[1] = 0x02
	binary.BigEndian.PutUint16(packet[2:], packetID)

	log.Printf("Sending UNSUBACK packet data: % X\n", packet)

	wrote, err := (*client.conn).Write(packet[:])
	if err != nil {
		err = errors.New(fmt.Sprintf("Error sending UNSUBACK: %s\n", err))
		client.cancel()
		return err
	}

	if wrote != len(packet) {
		err = errors.New(fmt.Sprintf("Error: UNSUBACK sent length mismatch. Sent: %d, Expected: %d\n", wrote, len(packet)))
		client.cancel()
		return err
	}

	log.Printf("UNSUBACK sent for Packet ID: %d\n", packetID)

	return nil
}

func sendPubRec(client *Client, packetID uint16) (err error) {
	var packet [4]byte
	packet[0] = PUBREC_PACKET_ID
	packet[1] = 0x02
	binary.BigEndian.PutUint16(packet[2:], packetID)

	log.Printf("Sending PUBREC packet data: % X\n", packet)

	wrote, err := (*client.conn).Write(packet[:])
	if err != nil {
		err = errors.New(fmt.Sprintf("Error sending PUBREC: %s\n", err))
		client.cancel()
		return err
	}

	if wrote != len(packet) {
		err = errors.New(fmt.Sprintf("Error: PUBREC sent length mismatch. Sent: %d, Expected: %d\n", wrote, len(packet)))
		client.cancel()
		return err
	}

	log.Printf("PUBREC sent for Packet ID: %d\n", packetID)

	return nil
}

func procPubRel(receivedData *[]byte, client *Client) (err error) {
	log.Printf("Received PUBREL packet data: % X\n", *receivedData)

	dataLength := len(*receivedData)
	if !(dataLength == 4) {
		err = errors.New(fmt.Sprintf("Error: Packet length invalid for PUBREL MQTT packet. Received data: % X\n", *receivedData))
		client.cancel()
		return err
	}

	packetID := binary.BigEndian.Uint16((*receivedData)[2:])

	message, exists := client.pubRelQueue.Load(packetID)
	if !exists {
		err = errors.New(fmt.Sprintf("Error: could not find payload and topic on PUB for packetID: %v\n", packetID))
		client.cancel()
		return err
	}

	err = sendPubComp(client, packetID)
	if err != nil {
		client.pubRelQueue.Delete(packetID)
		return err
	}

	err = queueMessageForDelivery(message.topic, message.payload)
	if err != nil {
		client.pubRelQueue.Delete(packetID)
		return err
	}

	log.Println("PUBREL processed deleting from pubRelQueue.")

	client.pubRelQueue.Delete(packetID)

	return nil
}

func sendPubComp(client *Client, packetID uint16) (err error) {
	var packet [4]byte
	packet[0] = PUBCOMP_PACKET_ID
	packet[1] = 0x02
	binary.BigEndian.PutUint16(packet[2:], packetID)

	wrote, err := (*client.conn).Write(packet[:])
	if err != nil {
		err = errors.New(fmt.Sprintf("Error sending PUBCOMP: %s\n", err))
		client.cancel()
		return err
	}

	if wrote != len(packet) {
		err = errors.New(fmt.Sprintf("Error: PUBCOMP sent length mismatch. Sent: %d, Expected: %d\n", wrote, len(packet)))
		client.cancel()
		return err
	}

	log.Printf("PUBCOMP sent for Packet ID: %d\n", packetID)

	return nil
}

func queueMessageForDelivery(topic string, payload []byte) (err error) {
	log.Printf("Delivering message to topic: %s, Payload: %s\n", topic, payload)
	return nil
}

func procConn(receivedData *[]byte, client *Client) {
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
		keepAlive:    int(binary.BigEndian.Uint16((*receivedData)[10:12])) + int(binary.BigEndian.Uint16((*receivedData)[10:12]))/2,
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
}

func sendConnack(client *Client, status byte) (err error) {
	var packet [4]byte

	packet[0] = CONNACK_PACKET_ID
	packet[1] = byte(0x2)

	if client.flags.cleanSession || status != CONNECTION_ACCEPTED_BYTE {
		packet[2] = byte(0x0)
	} else {
		packet[2] = byte(0x1)
	}

	packet[3] = status

	wrote, err := (*client.conn).Write(packet[:])
	if err != nil {
		err = errors.New(fmt.Sprintf("Error while writing packet: %s.\n", err))
		client.cancel()
		return err
	}

	if wrote != len(packet) {
		err = errors.New(fmt.Sprintf("Error: written byte differ in size to source byte data. Sent: %d Data length: %d.\n", wrote, len(packet)))
		client.cancel()
		return err
	}

	log.Printf("Connack packet sent. Packet:  % X\n", packet)

	return nil
}

func procPingreq(client *Client) (err error) {
	var packet [2]byte

	packet[0] = PINGRESP_PACKET_ID
	packet[1] = byte(0x0)

	wrote, err := (*client.conn).Write(packet[:])
	if err != nil {
		err = errors.New(fmt.Sprintf("Error while writing packet: %s.\n", err))
		client.cancel()
		return err
	}

	if wrote != len(packet) {
		err = errors.New(fmt.Sprintf("Error: written byte differ in size to source byte data. Sent: %d Data length: %d.\n", wrote, len(packet)))
		client.cancel()
		return err
	}

	client.connTicker.keepAliveCountdown = (*client.flags).keepAlive

	log.Printf("Pingresp packet sent. Packet:  % X\n", packet)

	return nil
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
					// todo race condition? acesso simultaneo com pingreq
					client.connTicker.keepAliveCountdown--
					if client.connTicker.keepAliveCountdown == 0 {
						client.cancel()
					}
				}
			}
		}
	}
}

func handleConnection(conn *net.Conn, appCtx *context.Context) {
	clientCtx, cancel := context.WithCancel(*appCtx)

	newClient := Client{
		id:          "unknown_" + strconv.Itoa(generateRandomId()),
		conn:        conn,
		ctx:         clientCtx,
		cancel:      cancel,
		outboxChan:  make(chan Packet, 100),
		inboxChan:   make(chan Packet, 100),
		connTicker:  &ConnTicker{},
		flags:       &Flags{},
		pubRelQueue: &TypedSyncMap[uint16, *Message]{},
	}

	go connInbox(&newClient)
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
