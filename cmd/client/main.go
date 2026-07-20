package main

import (
	"fmt"
	"os"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	url := "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(url)
	if err != nil {
		fmt.Println("Error connecting to rabbitmq")
		os.Exit(1)
	}

	fmt.Println("Connection successful")

	var username string
	for {
		username, err = gamelogic.ClientWelcome()
		if err != nil {
			fmt.Println("error getting username")
			continue
		}
		break
	}

	gameState := gamelogic.NewGameState(username)

	if err := pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, fmt.Sprintf("pause.%v", username), routing.PauseKey, pubsub.Transient, handlerPause(gameState)); err != nil {
		fmt.Println("Could not subscribe to pause")
		os.Exit(1)
	}

	if err := pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, fmt.Sprintf("army_moves.%v", username), "army_moves.*", pubsub.Transient, handlerMove(gameState)); err != nil {
		fmt.Println("Could not subscribe to army moves")
		os.Exit(1)
	}

	if err := pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, "war", fmt.Sprintf("%v.*", routing.WarRecognitionsPrefix), pubsub.Durable, handlerAllWar(gameState)); err != nil {
		fmt.Println("could not subscribe to war")
		os.Exit(1)
	}

	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("Error creating channel")
		os.Exit(1)
	}
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		firstWord := input[0]
		switch firstWord {
		case "spawn":
			if err := gameState.CommandSpawn(input); err != nil {
				fmt.Println("Error spawning:", err)
			}
		case "move":
			mv, err := gameState.CommandMove(input)
			if err != nil {
				fmt.Println("Errors executing move:", err)
			} else {
				if err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, fmt.Sprintf("army_moves.%v", username), mv); err != nil {
					fmt.Println("Error publishing move")
					continue
				}
				fmt.Printf("%v moved %v unit(s) to %v\n", mv.Player.Username, len(mv.Units), mv.ToLocation)
			}
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("invalid command")
		}
	}
}
