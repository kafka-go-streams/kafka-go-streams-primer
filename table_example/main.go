package main

import (
	"context"
	"fmt"

	streams "github.com/kafka-go-streams/kafka-go-streams"
	log "github.com/sirupsen/logrus"
)

func main() {

	rocksDB, err := streams.DefaultRocksDB("storage")

	if err != nil {
		log.Fatalf("Failed to construct rocks db")
	}

	table, err := streams.NewTable(&streams.TableConfig{
		Brokers: "localhost:9092",
		GroupID: "table_primer_group",
		Topic:   "test_topic",
		DB:      rocksDB,
		Context: context.Background(),
		Logger:  log.New(),
		Name:    "test_name",
	})

	if err != nil {
		log.Fatalf("%v\n", err)
	}

	fmt.Printf("%v\n", table)
}
