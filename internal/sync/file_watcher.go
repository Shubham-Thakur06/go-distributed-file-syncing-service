package sync

import (
	"context"
	"log"
	"path/filepath"
	"time"

	"github.com/Shubham-Thakur06/go-distributed-file-syncing-service/internal/utils"

	"github.com/fsnotify/fsnotify"
	"github.com/google/uuid"
)

type FileWatcher struct {
	watcher    *fsnotify.Watcher
	kafka      *utils.KafkaClient
	userID     string
	deviceID   string
	watchPaths map[string]bool
}

func NewFileWatcher(kafka *utils.KafkaClient, userID, deviceID string) (*FileWatcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	return &FileWatcher{
		watcher:    watcher,
		kafka:      kafka,
		userID:     userID,
		deviceID:   deviceID,
		watchPaths: make(map[string]bool),
	}, nil
}

func (fw *FileWatcher) AddPath(path string) error {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return err
	}

	if err := fw.watcher.Add(absPath); err != nil {
		return err
	}

	fw.watchPaths[absPath] = true
	return nil
}

func (fw *FileWatcher) RemovePath(path string) error {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return err
	}

	if err := fw.watcher.Remove(absPath); err != nil {
		return err
	}

	delete(fw.watchPaths, absPath)
	return nil
}

func (fw *FileWatcher) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil

		case event, ok := <-fw.watcher.Events:
			if !ok {
				return nil
			}

			// Skip temporary files and hidden files
			if filepath.Base(event.Name)[0] == '.' {
				continue
			}

			var changeType string
			switch {
			case event.Op&fsnotify.Create == fsnotify.Create:
				changeType = "CREATED"
			case event.Op&fsnotify.Write == fsnotify.Write:
				changeType = "MODIFIED"
			case event.Op&fsnotify.Remove == fsnotify.Remove:
				changeType = "DELETED"
			case event.Op&fsnotify.Rename == fsnotify.Rename:
				changeType = "RENAMED"
			default:
				continue
			}

			// Publish change to Kafka
			msg := &utils.FileChangeMessage{
				FileID:     uuid.New().String(),
				FilePath:   event.Name,
				ChangeType: changeType,
				Timestamp:  time.Now(),
				DeviceID:   fw.deviceID,
				UserID:     fw.userID,
			}

			if err := fw.kafka.PublishFileChange(ctx, msg); err != nil {
				log.Fatalf("Failed to publish file change: %v", err)
			}

		case err, ok := <-fw.watcher.Errors:
			if !ok {
				return nil
			}
			log.Fatalf("File watcher error: %v", err)
		}
	}
}

func (fw *FileWatcher) Close() error {
	return fw.watcher.Close()
}
