package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	connstr := "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connstr)
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()
	fmt.Println("Connected!")

	name, err := gamelogic.ClientWelcome()

	queueName := "pause." + name

	ch, q, err := pubsub.DeclareAndBind(
		conn,
		string(routing.ExchangePerilDirect),
		queueName,
		routing.PauseKey,
		pubsub.QueueTypeTransient,
	)
	if err != nil {
		log.Fatal(err)
	}

	defer ch.Close()
	fmt.Printf("Created queue: %s\n", q.Name)

	gameState := gamelogic.NewGameState(name)

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		"pause."+name,
		routing.PauseKey,
		pubsub.QueueTypeTransient,
		handlerPause(gameState),
	)

	if err != nil {
		log.Fatal(err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+name,
		routing.ArmyMovesPrefix+".*",
		pubsub.QueueTypeTransient,
		handlerMove(gameState, ch),
	)

	if err != nil {
		log.Fatal(err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		"war",
		"war.*",
		pubsub.QueueTypeDurable,
		handlerWar(gameState, ch),
	)

	if err != nil {
		log.Fatal(err)
	}

repl:
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		command := words[0]

		switch command {
		case "spawn":
			err = gameState.CommandSpawn(words)
			if err != nil {
				fmt.Printf("error occurred: %v\n", err)
				continue
			}
		case "move":
			move, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Printf("error occurred: %v\n", err)
				continue
			}

			err = pubsub.PublishJSON(
				ch,
				string(routing.ExchangePerilTopic),
				string(routing.ArmyMovesPrefix)+"."+name,
				move,
			)

			if err != nil {
				fmt.Printf("error occurred: %v\n", err)
				continue
			}

			fmt.Println("move successful", move)
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(words) < 2 {
				fmt.Println("spam requires 2 arguments")
				continue
			}

			count, err := strconv.Atoi(words[1])

			if err != nil {
				fmt.Printf("error converting to int, %v\n", err)
				continue
			}

			for range count {
				fmt.Println("creating spam log...")
				message := gamelogic.GetMaliciousLog()
				gameLog := routing.GameLog{
					CurrentTime: time.Now(),
					Message:     message,
					Username:    gameState.GetUsername(),
				}
				err := pubsub.PublishGob(
					ch,
					string(routing.ExchangePerilTopic),
					routing.GameLogSlug+"."+gameState.GetUsername(),
					gameLog,
				)
				if err != nil {
					fmt.Println("error occured publishing log")
					continue
				}
			}
		case "quit":
			gamelogic.PrintQuit()
			break repl
		default:
			fmt.Println("invalid command!")
			continue
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Signal received, program is shutting down...")
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")

		gs.HandlePause(ps)

		fmt.Println("ack!")
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")

		moveOutcome := gs.HandleMove(move)

		if moveOutcome == gamelogic.MoveOutcomeMakeWar {
			routingKey := routing.WarRecognitionsPrefix + "." + gs.GetUsername()

			err := pubsub.PublishJSON(
				ch,
				string(routing.ExchangePerilTopic),
				routingKey,
				gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)

			if err != nil {
				fmt.Printf("error: %s\n", err)
				return pubsub.NackRequeue
			}

			return pubsub.Ack
		} else if moveOutcome == gamelogic.MoveOutComeSafe {
			fmt.Println("ack!")
			return pubsub.Ack
		}

		fmt.Println("nackdiscard!")
		return pubsub.NackDiscard
	}
}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")

		var ackOutcome pubsub.AckType
		var gameLogMessage string
		outcome, winner, loser := gs.HandleWar(rw)

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			ackOutcome = pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			ackOutcome = pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			gameLogMessage = fmt.Sprintf("%s won a war against %s", winner, loser)
			ackOutcome = pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			gameLogMessage = fmt.Sprintf("%s won a war against %s", winner, loser)
			ackOutcome = pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			gameLogMessage = fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			ackOutcome = pubsub.Ack
		default:
			fmt.Println("error occured, outcome not recognized")
			ackOutcome = pubsub.NackDiscard
		}

		gameLog := routing.GameLog{
			CurrentTime: time.Now(),
			Message:     gameLogMessage,
			Username:    gs.GetUsername(),
		}

		err := pubsub.PublishGob(
			ch,
			string(routing.ExchangePerilTopic),
			routing.GameLogSlug+"."+gs.GetUsername(),
			gameLog,
		)
		if err != nil {
			fmt.Println("error occured publishing outcome")
			return pubsub.NackRequeue
		}

		return ackOutcome
	}

}
