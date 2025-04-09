package main

import (
	"fmt"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	broker   = "tcp://127.0.0.1:1234"
	topic    = "test"
	clientID = "test_client"
)

func main() {
	// Create an MQTT client options struct
	opts := mqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetClientID(clientID)
	opts.SetDefaultPublishHandler(messageHandler)
	opts.SetProtocolVersion(4) // MQTT 3.1.1
	// opts.SetOrderMatters(false) // Improve performance

	// Enable debugging logs
	opts.SetOnConnectHandler(func(client mqtt.Client) {
		fmt.Println("Connected to broker")
	})
	opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		fmt.Println("Connection lost:", err)
	})

	// Create an MQTT client
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		fmt.Println("Error connecting to broker:", token.Error())
		os.Exit(1)
	}

	// Subscribe to multiple topics
	topicFilters := map[string]byte{
		"topic/one":   0,
		"topic/two":   1,
		"topic/three": 2,
	}

	// Message handler
	callback := func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("Received on topic %s: %s\n", msg.Topic(), msg.Payload())
	}

	// Subscribe to all topics with the shared callback
	if token := client.SubscribeMultiple(topicFilters, callback); token.Wait() && token.Error() != nil {
		fmt.Println("Error subscribing to topics:", token.Error())
		os.Exit(1)
	}
	fmt.Println("Subscribed to topics:", topicFilters)

	// Subscribe to the topic
	if token := client.Subscribe(topic, 2, callback); token.Wait() && token.Error() != nil {
		fmt.Println("Error subscribing to topic:", token.Error())
		os.Exit(1)
	}
	fmt.Println("Subscribed to topic:", topic)

	// Publish a message every 5 seconds
	for i := 1; i <= 5; i++ {
		text := fmt.Sprintf("Message %d from Golang MQTT client", i)
		if token := client.Publish(topic, 2, false, text); token.Wait() && token.Error() != nil {
			fmt.Println("Error publishing message:", token.Error())
		} else {
			fmt.Println("Published:", text)
		}
		time.Sleep(5 * time.Second)
	}

	// Unsub to the topic
	if token := client.Unsubscribe(topic); token.Wait() && token.Error() != nil {
		fmt.Println("Error unsubscribing to topic:", token.Error())
		os.Exit(1)
	}
	fmt.Println("Unsubscribing to topic:", topic)

	// Unsubscribe from multiple topics
	topics := make([]string, 0, len(topicFilters))
	for topic := range topicFilters {
		topics = append(topics, topic)
	}

	if token := client.Unsubscribe(topics...); token.Wait() && token.Error() != nil {
		fmt.Println("Error unsubscribing from topics:", token.Error())
		os.Exit(1)
	}
	fmt.Println("Unsubscribed from topics:", topics)

	// Disconnect from the broker
	client.Disconnect(250)
	fmt.Println("Disconnected")
}

var messageHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
}
