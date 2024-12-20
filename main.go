package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/pubsub"
)

func main() {
	ctx := context.Background()
	ctx, _ = context.WithTimeout(ctx, time.Second*10)
	projectId := "rhamilton-001"

	client, err := pubsub.NewClient(ctx, projectId)
	if err != nil {
		log.Fatalf("Unable to create new client: %v", err)
	}
	defer client.Close()

	subscriptions := client.Subscriptions(ctx)
	for s, _ := subscriptions.Next(); s != nil; s, _ = subscriptions.Next() {
		if s.ID() != "sub1112" {
			continue
		}

		fmt.Println(s.ID())

		dctx, _ := context.WithDeadline(ctx, time.Now().Add(time.Second*10))
		log.Print("Starting receive")
		messagesToAck := []*pubsub.Message{}

		someChannel := make(chan *pubsub.Message)
		go func() {
			messageCollector := []*pubsub.Message{}
			for len(messageCollector) < 5 && dctx.Err() != context.Canceled {
				m := <-someChannel
				fmt.Printf("Acking %s\n", m.ID)
				messageCollector = append(messageCollector, m)
			}

			for _, m := range messageCollector {
				fmt.Printf("Acked %s\n", m.ID)
				m.Ack()
			}
		}()

		s.Receive(dctx, func(ctx context.Context, m *pubsub.Message) {
			fmt.Println(string(m.Data))
			m.Ack()
		})

		for _, m := range messagesToAck {
			fmt.Printf("We will ack %s shortly\n", m.Data)
		}

		for _, m := range messagesToAck {
			fmt.Printf("Acking %s\n", m.Data)
			m.Ack()
		}

		log.Print("Receive complete")
	}
}
