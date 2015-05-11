package main

import (
	"github.com/Shopify/sarama"
	"os"
	"os/signal"
	"sync"
	"log"
	"fmt"
	"time"
)

func connect(ip []string, config *sarama.Config)(sarama.AsyncProducer, error) {
	now := time.Now()

	producer, err := sarama.NewAsyncProducer(ip, config)
	if err != nil && err == sarama.ErrOutOfBrokers {
		fmt.Println("Can't connect. Retrying...")
		time.Sleep(10 * time.Second)
		return connect(ip, config)
	}

	fmt.Println("total time: ", time.Since(now).Seconds())
	fmt.Println("connected")
	return producer, err
}

func kafkaProducer(ip []string, topic string, msgChannel chan []byte) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 10

	producer, err := connect(ip, config)
	if err != nil {
		panic(err)
	}

	// Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var (
		wg sync.WaitGroup
		enqueued, successes, errors int
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _ = range producer.Successes() {
			successes++
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _ = range producer.Errors() {
			log.Println(err)
			errors++
		}
	}()

	ProducerLoop:
	for msg := range msgChannel {
		message := &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(msg)}
		select {
		case producer.Input() <- message:
			enqueued++

		case <- signals:
			producer.AsyncClose()
			break ProducerLoop

		}
	}

	wg.Wait()

	log.Printf("Successfully produced: %d; errors: %d\n", successes, errors)
}

func main() {
	ip := []string{"localhost:9092"}

	myChannel := make(chan []byte, 3)
	myChannel <- []byte("Hello Apache")
	myChannel <- []byte("Hello Kafka")
	myChannel <- []byte("Hello Cassandra")

	kafkaProducer(ip, "test", myChannel)
}