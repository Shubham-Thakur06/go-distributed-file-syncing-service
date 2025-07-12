package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	FileChangesTopic = "file-changes"
)

type KafkaClient struct {
	writer *kafka.Writer
	reader *kafka.Reader
}

type FileChangeMessage struct {
	FileID     string    `json:"file_id"`
	FilePath   string    `json:"file_path"`
	ChangeType string    `json:"change_type"`
	Timestamp  time.Time `json:"timestamp"`
	DeviceID   string    `json:"device_id"`
	VersionID  string    `json:"version_id"`
	UserID     string    `json:"user_id"`
}

func NewKafkaClient(brokers []string) (*KafkaClient, error) {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    FileChangesTopic,
		Balancer: &kafka.LeastBytes{},
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       FileChangesTopic,
		StartOffset: kafka.LastOffset,
		GroupID:     "file-sync-group",
	})

	return &KafkaClient{
		writer: writer,
		reader: reader,
	}, nil
}

func (k *KafkaClient) PublishFileChange(ctx context.Context, msg *FileChangeMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	err = k.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(msg.FileID),
		Value: data,
	})
	if err != nil {
		return fmt.Errorf("failed to write message: %v", err)
	}

	return nil
}

func (k *KafkaClient) SubscribeToFileChanges(ctx context.Context, userID string, callback func(*FileChangeMessage)) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			msg, err := k.reader.ReadMessage(ctx)
			if err != nil {
				log.Fatalf("Failed to read Kafka message: %v", err)
				continue
			}

			var fileChange FileChangeMessage
			if err := json.Unmarshal(msg.Value, &fileChange); err != nil {
				log.Fatalf("Failed to unmarshal file change message: %v", err)
				continue
			}

			if fileChange.UserID == userID {
				callback(&fileChange)
			}
		}
	}
}

func (k *KafkaClient) Close() error {
	if err := k.writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %v", err)
	}
	if err := k.reader.Close(); err != nil {
		return fmt.Errorf("failed to close reader: %v", err)
	}
	return nil
}
