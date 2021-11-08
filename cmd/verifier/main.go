package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/hi-rustin/ticdc-old-value-kafka-integration-test/internal/verifier"
)

const kafkaGroupName = "old-value-group"

type options struct {
	brokers string
	topics  string
}

// validate validates grower options.
func (o *options) validate() error {
	if o.brokers == "" {
		return errors.New("no brokers specified")
	}
	if o.topics == "" {
		return errors.New("no topics specified")
	}
	return nil
}

func gatherOptions() options {
	o := options{}
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.StringVar(&o.brokers, "brokers", "", "Kafka bootstrap brokers to connect to, as a comma separated list")
	fs.StringVar(&o.topics, "topics", "", "Kafka topics to be consumed, as a comma separated list")
	_ = fs.Parse(os.Args[1:])
	return o
}

func main() {
	o := gatherOptions()
	if err := o.validate(); err != nil {
		log.Panicf("Invalid options: %v", err)
	}

	config := sarama.NewConfig()

	consumer := verifier.New()

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(strings.Split(o.brokers, ","), kafkaGroupName, config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims.
			if err := client.Consume(ctx, strings.Split(o.topics, ","), consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// Check if context was cancelled, signaling that the consumer should stop.
			if ctx.Err() != nil {
				return
			}
			consumer.Ready = make(chan bool)
		}
	}()

	// Until the consumer has been set up.
	<-consumer.Ready

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-ctx.Done():
		log.Println("Terminating: context cancelled")
	case <-sigterm:
		log.Println("Terminating: via signal")
	}

	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}
