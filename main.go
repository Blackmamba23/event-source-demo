package main

import (
	"event-source-demo/internal/employee"
	"event-source-demo/pkg/service"
	"log"

	"github.com/NYTimes/gizmo/config"
	"github.com/NYTimes/gizmo/pubsub"
	"github.com/NYTimes/gizmo/pubsub/kafka"
	"github.com/NYTimes/gizmo/server"
	badger "github.com/dgraph-io/badger/v2"
)

func main() {
	var cfg struct {
		Server *server.Config
		Kafka  *kafka.Config
	}

	// embebedd KV DB
	db, err := badger.Open(badger.DefaultOptions("badger"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	config.LoadJSONFile("./config.json", &cfg)
	streamService := service.NewStreamService(cfg.Server.HTTPPort, cfg.Kafka, employee.NewRepository(db))
	apiDone := make(chan bool, 1)
	employeeConsumerDone := make(chan bool, 1)

	// API thread
	go func() {
		defer func() {
			apiDone <- true
			server.Log.WithField("topic", "Server").Info("closing API thread")
		}()

		// set the pubsub's Log to be the same as server's
		pubsub.Log = server.Log

		// in case we want to override the port or log location via CLI
		server.SetConfigOverrides(cfg.Server)

		server.Init("event-sourcing-example", cfg.Server)

		err = server.Register(streamService)
		if err != nil {
			server.Log.Fatal(err)
		}

		if err = server.Run(); err != nil {
			server.Log.Fatal(err)
		}
	}()

	// employee consumer thread
	go func() {
		defer func() {
			employeeConsumerDone <- true
			server.Log.WithField("topic", "Consumer").Info("closing employee consumer thread")
		}()
		streamService.ConsumeEmployeeTopic()
	}()

	<-apiDone
	<-employeeConsumerDone

}
