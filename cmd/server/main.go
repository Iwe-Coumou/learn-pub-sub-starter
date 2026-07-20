package main

import (
	"fmt"
	"os"

	//"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	url := "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(url)
	if err != nil {
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("Connection successful")

	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("Error creating channel")
		os.Exit(1)
	}

	_, q, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, "game_logs", routing.GameLogSlug, pubsub.Durable)
	if err != nil {
		fmt.Println("Error declaring and binding queue")
		os.Exit(1)
	}
	fmt.Printf("Queue %v declared and bound!\n", q.Name)

	gamelogic.PrintServerHelp()

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		firstWord := input[0]
		switch firstWord {
		case "pause":
			fmt.Println("Sending pause message")

			state := routing.PlayingState{IsPaused: true}
			if err := pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, state); err != nil {
				fmt.Println("Error publishing message")
				os.Exit(1)
			}
		case "resume":
			fmt.Println("Sending resume message")
			state := routing.PlayingState{IsPaused: false}
			if err := pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, state); err != nil {
				fmt.Println("Error publishing message")
				os.Exit(1)
			}
		case "quit":
			fmt.Println("Quitting")
			return
		default:
			fmt.Println("invalid command")
		}
	}
}
