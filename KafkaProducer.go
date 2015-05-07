package main

import (
	"github.com/Shopify/sarama"
	"os"
	"os/signal"
	"sync"
	"log"
)

func main() {

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, config)
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
	for{
		message := &sarama.ProducerMessage{Topic: "my-replicated-topic", Value: sarama.StringEncoder("loop msg")}
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