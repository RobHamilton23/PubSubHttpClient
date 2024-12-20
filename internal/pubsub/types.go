package pubsub

import (
	"encoding/base64"
	"fmt"
	"log/slog"
)

type PubSubPullResponse struct {
	ReceivedMessages []PubSubMessage `json:"receivedMessages"`
}

type PubSubMessage struct {
	AckId   string `json:"ackId"`
	Message struct {
		DataBase64  string `json:"data"`
		MessageId   string `json:"messageId"`
		PublishTime string `json:"publishTime"`
	} `json:"message"`
	DeliveryAttempt int `json:"deliveryAttempt"`
}

func (psm *PubSubMessage) Data() (string, error) {
	decodedBytes, err := base64.StdEncoding.DecodeString(psm.Message.DataBase64)
	if err != nil {
		slog.With("base64", psm.Message.DataBase64).With("error", err).Error("Unable to decode base64")
		return "", fmt.Errorf("unable to decode base64: %v", err)
	}

	return string(decodedBytes), nil
}
