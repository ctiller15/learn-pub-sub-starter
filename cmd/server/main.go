package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connstr := "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connstr)
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()
	fmt.Println("Server starting up...")

	newChan, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}

	_, q, err := pubsub.DeclareAndBind(conn, string(routing.ExchangePerilTopic), routing.GameLogSlug, routing.GameLogSlug+".*", pubsub.QueueTypeDurable)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Queue %v declared and bound!\n", q.Name)

	err = pubsub.SubscribeGob(
		conn,
		routing.ExchangePerilTopic,
		"game_logs",
		"game_logs.*",
		pubsub.QueueTypeDurable,
		handlerLogs(),
	)

	gamelogic.PrintServerHelp()
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		} else if words[0] == "pause" {
			fmt.Println("sending a pause message!")

			jsonData, err := json.Marshal(routing.PlayingState{IsPaused: true})

			err = pubsub.PublishJSON(newChan,
				string(routing.ExchangePerilDirect),
				routing.PauseKey,
				jsonData)

			if err != nil {
				log.Fatal(err)
			}
		} else if words[0] == "resume" {
			fmt.Println("sending a resume message!")

			jsonData, err := json.Marshal(routing.PlayingState{IsPaused: false})

			err = pubsub.PublishJSON(newChan,
				string(routing.ExchangePerilDirect),
				routing.PauseKey,
				jsonData)

			if err != nil {
				log.Fatal(err)
			}
		} else if words[0] == "quit" {
			fmt.Println("exiting...")
			break
		} else {
			fmt.Println("I don't understand...")
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Signal received, program is shutting down...")
}

func handlerLogs() func(routing.GameLog) pubsub.AckType {
	return func(gl routing.GameLog) pubsub.AckType {
		defer fmt.Print("> ")
		err := gamelogic.WriteLog(gl)
		if err != nil {
			fmt.Println("error occured writing log")
			return pubsub.NackRequeue
		}

		return pubsub.Ack
	}
}
