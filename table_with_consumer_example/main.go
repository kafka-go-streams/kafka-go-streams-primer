package main

import (
	"bytes"
	"context"
	"flag"

	streams "github.com/kafka-go-streams/kafka-go-streams"
	log "github.com/sirupsen/logrus"
	k "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var storage_path = flag.String("storage", "storage", "Path to storage")

func main() {
	flag.Parse()

	rocksDB, err := streams.DefaultRocksDB(*storage_path)

	if err != nil {
		log.Fatalf("Failed to construct rocks db")
	}

	brokers := "localhost:9092"
	groupId := "table_primer_group"

	logger := log.New()
	logger.SetLevel(log.TraceLevel)

	log.Printf("Created rocksdb: %v", rocksDB)

	table, err := streams.NewTable(&streams.TableConfig{
		Brokers: brokers,
		GroupID: groupId,
		Topic:   "test_topic",
		DB:      rocksDB,
		Context: context.Background(),
		Logger:  logger,
		Name:    "test_name",
	})
	if err != nil {
		log.Fatalf("%v\n", err)
	}

	consumer, err := k.NewConsumer(&k.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           groupId,
		"enable.auto.commit": false,
	})

	if err != nil {
		log.Fatalf("%v\n", err)
	}

	consumer.Subscribe("input_topic", func(c *k.Consumer, e k.Event) error {
		log.Printf("Rebalancing event: %v", e)
		return nil
	})

	log.Printf("Starting consumer poll")
	for {
		e := consumer.Poll(2000)
		switch v := e.(type) {
		case *k.Message:
			log.Printf("Received message with key: %s", v.Key)
			tableValue := table.Get(v.Key)
			log.Printf("Joined value: %s", bytes.Join([][]byte{tableValue, v.Value}, []byte(" -- ")))
		case *k.Error:
			if v.Code() != k.ErrTimedOut {
				log.Errorf("Error receiving message: %v", v)
			}
		default:
			log.Printf("Unknown event type: %v\n", v)
		}
	}

	//fmt.Printf("%v\n", table)
}
