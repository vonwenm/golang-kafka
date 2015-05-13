package main

import (
	"github.com/Shopify/sarama"
	"os"
	"os/signal"
	"fmt"
	"time"
	"strconv"
	"sync"
	"github.com/braindev/gopool"
)

func connectTestAsync(ip []string, config *sarama.Config)(sarama.AsyncProducer) {
	now := time.Now()

	producer, err := sarama.NewAsyncProducer(ip, config)
	if err != nil && err == sarama.ErrOutOfBrokers {
		fmt.Println("Can't connect. Retrying...")
		time.Sleep(10 * time.Second)
		return connectTestAsync(ip, config)
	}

	fmt.Println("total time: ", time.Since(now).Seconds())
	fmt.Println("connected")
	return producer
}

func producerTestAsync(ip []string, topic string) (func([]byte) string) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 10

	resourceOpen := func()(interface{}) {
		return connectTestAsync(ip, config)
	}

	resourceClose := func(r interface{}) {
		producer := r.(sarama.AsyncProducer)
		producer.Close()
	}

	pool.Initialize("producer", 10, 10, resourceOpen, resourceClose)
	rp := pool.Name("producer")

	// Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	return func(msg []byte) string {
		resourceWrapper := rp.Acquire()

		defer rp.Release(resourceWrapper)

		producer := resourceWrapper.(sarama.AsyncProducer)

		message := &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(msg)}
		select {
			case producer.Input() <- message: {
				select {
					case bla := <- producer.Successes():
						x,_ := bla.Value.Encode()
						return fmt.Sprintln(string(msg), string(x))
					case <- producer.Errors(): return "nok"
				}
			}
			case <- signals: rp.Destroy("producer")
		}

		return "nok"
	}
}

func sendMessagesAsync(producer func([]byte) string, total int) {
	var wg sync.WaitGroup
	wg.Add(total)

	for i:=0; i<100; i++ {
		go func(i int) {
			defer wg.Done()
			fmt.Println(producer([]byte("hi " + strconv.Itoa(i))))
		}(i)
	}

	wg.Wait()
}

func main() {
	ip := []string{"localhost:9092"}
	warmUp, total := 10000, 1000000

	producer := producerTestAsync(ip, "test")
	sendMessagesAsync(producer, warmUp)

	now := time.Now()

	sendMessagesAsync(producer, total)

	fmt.Println("total time: ", time.Since(now).Seconds())
}
