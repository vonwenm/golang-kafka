package main

import (
	"github.com/Shopify/sarama"
	"os"
	"os/signal"
	"fmt"
	"time"
	"strconv"
	"sync"
)

func connectTest(ip []string, config *sarama.Config)(sarama.SyncProducer, error) {
	producer, err := sarama.NewSyncProducer(ip, config)
	if err != nil && err == sarama.ErrOutOfBrokers {
		fmt.Println("Can't connect. Retrying...")
		time.Sleep(10 * time.Second)
		return connectTest(ip, config)
	}

	fmt.Println("connected")
	return producer, err
}

func producerTest(ip []string, topic string) (func([]byte)) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 10

	producer, err := connectTest(ip, config)
	if err != nil {
		panic(err)
	}

	// Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	return func(msg []byte) {
		message := &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(msg)}
		producer.SendMessage(message)
	}
}

func sendMessages(producer func([]byte), total int) {
	var wg sync.WaitGroup
	wg.Add(total)

	for i:=0; i<total; i++ {
		go func(i int) {
			defer wg.Done()
			producer([]byte("hi " + strconv.Itoa(i)))
		}(i)
	}

	wg.Wait()
}

func main() {
	ip := []string{"localhost:9092"}
	warmUp, total := 10000, 1000000

	producer := producerTest(ip, "test")
	sendMessages(producer, warmUp)

	now := time.Now()

	sendMessages(producer, total)

	fmt.Println("total time: ", time.Since(now).Seconds())
}