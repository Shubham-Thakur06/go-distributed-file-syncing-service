package sync

import (
	"context"
	"log"
	"time"

	"github.com/Shubham-Thakur06/go-distributed-file-syncing-service/internal/models"
	"github.com/Shubham-Thakur06/go-distributed-file-syncing-service/internal/proto"
	"github.com/Shubham-Thakur06/go-distributed-file-syncing-service/internal/utils"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

type SyncService struct {
	proto.UnimplementedSyncServiceServer
	db          *gorm.DB
	kafka       *utils.KafkaClient
	fileWatcher *FileWatcher
}

func NewSyncService(db *gorm.DB, kafka *utils.KafkaClient) *SyncService {
	return &SyncService{
		db:    db,
		kafka: kafka,
	}
}

func (s *SyncService) SyncFile(ctx context.Context, req *proto.SyncRequest) (*proto.SyncResponse, error) {
	var file models.File
	if err := s.db.Preload("Versions").First(&file, "id = ?", req.FileId).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return &proto.SyncResponse{
				Status:  proto.SyncResponse_ERROR,
				Message: "File not found",
			}, nil
		}
		return nil, err
	}

	latestVersion := file.Versions[len(file.Versions)-1]

	if latestVersion.Hash == req.FileHash {
		return &proto.SyncResponse{
			Status:          proto.SyncResponse_SYNCED,
			Message:         "File is up to date",
			LatestVersionId: latestVersion.ID,
		}, nil
	}

	if latestVersion.Hash != req.FileHash && latestVersion.DeviceID != req.DeviceId {
		return &proto.SyncResponse{
			Status:          proto.SyncResponse_CONFLICT,
			Message:         "Conflict detected",
			LatestVersionId: latestVersion.ID,
		}, nil
	}

	return &proto.SyncResponse{
		Status:          proto.SyncResponse_NEEDS_UPDATE,
		Message:         "File needs update",
		LatestVersionId: latestVersion.ID,
	}, nil
}

func (s *SyncService) GetFileVersions(ctx context.Context, req *proto.FileVersionRequest) (*proto.FileVersionResponse, error) {
	var file models.File
	if err := s.db.Preload("Versions").First(&file, "id = ?", req.FileId).Error; err != nil {
		return nil, status.Errorf(codes.NotFound, "file not found: %v", err)
	}

	versions := make([]*proto.FileVersion, len(file.Versions))
	for i, v := range file.Versions {
		versions[i] = &proto.FileVersion{
			VersionId: v.ID,
			CreatedAt: v.CreatedAt.Format(time.RFC3339),
			DeviceId:  v.DeviceID,
			Size:      v.Size,
			Hash:      v.Hash,
		}
	}

	return &proto.FileVersionResponse{
		Versions: versions,
	}, nil
}

func (s *SyncService) ResolveConflict(ctx context.Context, req *proto.ConflictResolutionRequest) (*proto.ConflictResolutionResponse, error) {
	err := s.db.Transaction(func(tx *gorm.DB) error {
		newVersion := &models.FileVersion{
			ID:       uuid.New().String(),
			FileID:   req.FileId,
			DeviceID: "system", // Mark as system-resolved
		}

		if err := tx.Create(newVersion).Error; err != nil {
			return err
		}

		if err := tx.Model(&models.FileVersion{}).
			Where("id IN ?", req.LosingVersionIds).
			Update("resolved_by", newVersion.ID).Error; err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to resolve conflict: %v", err)
	}

	return &proto.ConflictResolutionResponse{
		Success:      true,
		Message:      "Conflict resolved successfully",
		NewVersionId: uuid.New().String(),
	}, nil
}

func (s *SyncService) WatchFileChanges(req *proto.WatchRequest, stream proto.SyncService_WatchFileChangesServer) error {
	ctx := stream.Context()

	watcher, err := NewFileWatcher(s.kafka, req.UserId, req.DeviceId)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create file watcher: %v", err)
	}
	defer watcher.Close()

	for _, path := range req.FolderPaths {
		if err := watcher.AddPath(path); err != nil {
			log.Fatalf("Failed to add watch path: %s", path)
		}
	}

	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go watcher.Start(watchCtx)

	return s.kafka.SubscribeToFileChanges(ctx, req.UserId, func(msg *utils.FileChangeMessage) {
		if msg.DeviceID == req.DeviceId {
			return
		}

		if err := s.handleFileChange(ctx, msg); err != nil {
			log.Fatalf("Failed to handle file change: %v", err)
			return
		}

		event := &proto.FileChangeEvent{
			FileId:    msg.FileID,
			FilePath:  msg.FilePath,
			DeviceId:  msg.DeviceID,
			Timestamp: msg.Timestamp.Format(time.RFC3339),
			VersionId: msg.VersionID,
		}

		switch msg.ChangeType {
		case "CREATED":
			event.ChangeType = proto.FileChangeEvent_CREATED
		case "MODIFIED":
			event.ChangeType = proto.FileChangeEvent_MODIFIED
		case "DELETED":
			event.ChangeType = proto.FileChangeEvent_DELETED
		case "RENAMED":
			event.ChangeType = proto.FileChangeEvent_RENAMED
		}

		if err := stream.Send(event); err != nil {
			log.Fatalf("Failed to send file change event: %v", err)
			cancel()
		}
	})
}

func (s *SyncService) handleFileChange(ctx context.Context, msg *utils.FileChangeMessage) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		file := &models.File{
			ID:      msg.FileID,
			Path:    msg.FilePath,
			OwnerID: msg.UserID,
		}

		if err := tx.Save(file).Error; err != nil {
			return err
		}

		version := &models.FileVersion{
			ID:       uuid.New().String(),
			FileID:   msg.FileID,
			DeviceID: msg.DeviceID,
		}

		if err := tx.Create(version).Error; err != nil {
			return err
		}

		return nil
	})
}
