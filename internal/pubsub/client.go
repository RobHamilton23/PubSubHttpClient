package pubsub

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"cloud.google.com/go/pubsub"
	"golang.org/x/oauth2/google"
)

type PubSubHttpClient struct {
	httpClient *http.Client
}

func NewPubSubHttpClient() (*PubSubHttpClient, error) {
	creds, err := getDefaultCredentials()
	if err != nil {
		slog.With("error", err).Error("Unable to get default credentials")
		return nil, fmt.Errorf("unable to get default credentials: %v", err)
	}

	return NewPubSubHttpClientWithCredentials(creds)
}

func NewPubSubHttpClientWithCredentials(creds *google.Credentials) (*PubSubHttpClient, error) {
	token, err := getTokenFromCredentials(creds)
	if err != nil {
		slog.With("error", err).Error("Unable to get token")
		return nil, fmt.Errorf("unable to get token: %v", err)
	}

	httpClient, err := getHttpClientWithToken(token)
	if err != nil {
		slog.With("error", err).Error("Unable to get http client")
		return nil, fmt.Errorf("unable to get http client: %v", err)
	}

	return &PubSubHttpClient{
		httpClient: httpClient,
	}, nil
}

func (ps *PubSubHttpClient) FetchMessages(project string, subscription string, maxMessages int) (*PubSubPullResponse, error) {
	postBody := fmt.Sprintf(`{"maxMessages": %d}`, maxMessages)
	postBodyBytes := []byte(postBody)

	resp, err := ps.httpClient.Post(
		getPubSubPullUrl(project, subscription, "pull"),
		"application/json",
		bytes.NewBuffer(postBodyBytes),
	)
	if err != nil {
		slog.With("error", err).Error("Unable to make request")
		return nil, fmt.Errorf("unable to make request: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		slog.With("status", resp.Status).Error("Unexpected status code")
		respBodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			slog.With("error", err).Error("Unable to read response body")
		}

		errorLogger := slog.With("status", resp.Status)
		var respBody string
		if len(respBodyBytes) > 0 {
			respBody = string(respBodyBytes)
			errorLogger = errorLogger.With("body", respBody)
		}

		errorLogger.Error("Unexpected status code")
		return nil, fmt.Errorf("unexpected status code: %v, body: %s", resp.Status, respBody)
	}

	responseContentType := resp.Header.Get("Content-Type")
	if !strings.Contains(responseContentType, "application/json") {
		slog.With("content-type", responseContentType).Error("Unexpected content type")
		return nil, fmt.Errorf("unexpected content type: %v", responseContentType)
	}

	var response *PubSubPullResponse
	respBodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		slog.With("error", err).Error("Unable to read response body")
		return nil, fmt.Errorf("unable to read response body: %v", err)
	}
	defer resp.Body.Close()

	err = json.Unmarshal(respBodyBytes, &response)
	if err != nil {
		slog.With("error", err).Error("Unable to unmarshal response")
		return nil, fmt.Errorf("unable to unmarshal response: %v", err)
	}

	return response, nil
}

func (ps *PubSubHttpClient) ModifyAckDeadline(project string, subscription string, ackIds []string, ackDeadlineSeconds int) error {
	if len(ackIds) == 0 {
		slog.Error("No ackIds provided")
		return errors.New("no ackIds provided")
	}

	ackIdsJson, err := json.Marshal(ackIds)
	if err != nil {
		slog.With("error", err).Error("Unable to marshal ackIds to JSON")
		return fmt.Errorf("unable to marshal ackIds to JSON: %v", err)
	}
	postBody := fmt.Sprintf(`{"ackIds": %s, "ackDeadlineSeconds": %d}`, ackIdsJson, ackDeadlineSeconds)
	fmt.Print(postBody)
	postBodyBytes := []byte(postBody)

	resp, err := ps.httpClient.Post(
		getPubSubPullUrl(project, subscription, "modifyAckDeadline"),
		"application/json",
		bytes.NewBuffer(postBodyBytes),
	)
	if err != nil {
		slog.With("error", err).Error("Unable to make request")
		return fmt.Errorf("unable to make request: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		slog.With("status", resp.Status).Error("Unexpected status code")
		respBodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			slog.With("error", err).Error("Unable to read response body")
		}

		errorLogger := slog.With("status", resp.Status)
		var respBody string
		if len(respBodyBytes) > 0 {
			respBody = string(respBodyBytes)
			errorLogger = errorLogger.With("body", respBody)
		}

		errorLogger.Error("Unexpected status code")
		return fmt.Errorf("unexpected status code: %v, body: %s", resp.Status, respBody)
	}

	return nil
}

func (ps *PubSubHttpClient) AcknowledgeMessages(project string, subscription string, ackIds []string) error {
	if len(ackIds) == 0 {
		slog.Error("No ackIds provided")
		return errors.New("no ackIds provided")
	}

	ackIdsJson, err := json.Marshal(ackIds)
	if err != nil {
		slog.With("error", err).Error("Unable to marshal ackIds to JSON")
		return fmt.Errorf("unable to marshal ackIds to JSON: %v", err)
	}
	postBody := fmt.Sprintf(`{"ackIds": %s}`, ackIdsJson)
	postBodyBytes := []byte(postBody)

	resp, err := ps.httpClient.Post(
		getPubSubPullUrl(project, subscription, "acknowledge"),
		"application/json",
		bytes.NewBuffer(postBodyBytes),
	)
	if err != nil {
		slog.With("error", err).Error("Unable to make request")
		return fmt.Errorf("unable to make request: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		slog.With("status", resp.Status).Error("Unexpected status code")
		respBodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			slog.With("error", err).Error("Unable to read response body")
		}

		errorLogger := slog.With("status", resp.Status)
		var respBody string
		if len(respBodyBytes) > 0 {
			respBody = string(respBodyBytes)
			errorLogger = errorLogger.With("body", respBody)
		}

		errorLogger.Error("Unexpected status code")
		return fmt.Errorf("unexpected status code: %v, body: %s", resp.Status, respBody)
	}

	return nil
}

func getDefaultCredentials() (*google.Credentials, error) {
	creds, err := google.FindDefaultCredentials(context.Background(), pubsub.ScopePubSub)
	if err != nil {
		slog.With("error", err).Error("Unable to find default credentials")
		return nil, fmt.Errorf("unable to find default credentials: %v", err)
	}
	return creds, nil
}

func getTokenFromCredentials(creds *google.Credentials) (string, error) {
	token, err := creds.TokenSource.Token()
	if err != nil {
		slog.With("error", err).Error("Unable to get token")
		return "", fmt.Errorf("unable to get token: %v", err)
	}
	return token.AccessToken, nil
}

type gcpRoundTripper struct {
	token string
}

func (rt *gcpRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if rt.token == "" {
		return nil, errors.New("no token provided")
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", rt.token))
	return http.DefaultTransport.RoundTrip(req)
}

func getHttpClientWithToken(token string) (*http.Client, error) {
	gcpHttpClient := http.Client{}
	gcpHttpClient.Transport = &gcpRoundTripper{token: token}
	return &gcpHttpClient, nil
}

func getPubSubPullUrl(projectId string, subscriptionId string, operation string) string {
	return fmt.Sprintf("https://pubsub.googleapis.com/v1/projects/%s/subscriptions/%s:%s", projectId, subscriptionId, operation)
}
