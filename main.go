package main

import (
	"fmt"
	"log/slog"
	"monoraillime/pubsubReceiver/internal/pubsub"
)

func main() {
	projectId := "rhamilton-001"

	pubsubClient, err := pubsub.NewPubSubHttpClient()
	if err != nil {
		slog.With("error", err).Error("Unable to create pubsub client")
		panic(err)
	}

	messages, err := pubsubClient.FetchMessages(projectId, "sub1112", 20)
	if err != nil {
		slog.With("error", err).Error("Unable to fetch messages")
		panic(err)
	}

	ackIdsToDelay := []string{}

	for _, m := range messages.ReceivedMessages {
		decodedMessage, _ := m.Data()
		ackIdsToDelay = append(ackIdsToDelay, m.AckId)
		fmt.Printf("%v\t%s\n", m.DeliveryAttempt, decodedMessage)
	}

	if len(messages.ReceivedMessages) > 0 {
		err = pubsubClient.ModifyAckDeadline(projectId, "sub1112", ackIdsToDelay, 60)
		if err != nil {
			slog.With("error", err).Error("Unable to modify ack deadline")
			panic(err)
		}
	}

	if len(messages.ReceivedMessages) > 0 {
		err = pubsubClient.AcknowledgeMessages(projectId, "sub1112", ackIdsToDelay)
		if err != nil {
			slog.With("error", err).Error("Unable to acknowledge messages")
			panic(err)
		}
	}
}
