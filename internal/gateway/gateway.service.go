package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"

	"github.com/Shubham-Thakur06/go-distributed-file-syncing-service/internal/models"
	"github.com/Shubham-Thakur06/go-distributed-file-syncing-service/internal/proto"
	"github.com/Shubham-Thakur06/go-distributed-file-syncing-service/internal/utils"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

const chunkSize = 1024 * 1024 // 1MB chunks

type FileGatewayService struct {
	proto.UnimplementedFileServiceServer
	db       *gorm.DB
	s3Client *utils.S3Client
}

func NewFileGatewayService(db *gorm.DB, s3Client *utils.S3Client) *FileGatewayService {
	return &FileGatewayService{
		db:       db,
		s3Client: s3Client,
	}
}

func (s *FileGatewayService) UploadFile(stream proto.FileService_UploadFileServer) error {
	firstChunk, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.Internal, "failed to receive file chunk: %v", err)
	}

	fileID := firstChunk.FileId
	if fileID == "" {
		fileID = uuid.New().String()
	}

	var buffer bytes.Buffer
	totalSize := int64(0)
	hasher := sha256.New()

	buffer.Write(firstChunk.Content)
	hasher.Write(firstChunk.Content)
	totalSize += int64(len(firstChunk.Content))

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "failed to receive chunk: %v", err)
		}

		buffer.Write(chunk.Content)
		hasher.Write(chunk.Content)
		totalSize += int64(len(chunk.Content))
	}

	// content type detection
	contentType := http.DetectContentType(buffer.Bytes())

	fileHash := hex.EncodeToString(hasher.Sum(nil))

	s3Key := utils.GenerateS3Key(firstChunk.UserId, firstChunk.DeviceId, firstChunk.FileName)

	err = s.s3Client.UploadFile(context.Background(), s3Key, &buffer)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to upload to S3: %v", err)
	}

	file := &models.File{
		ID:          fileID,
		Name:        firstChunk.FileName,
		Path:        s3Key,
		Size:        totalSize,
		ContentType: contentType,
		OwnerID:     firstChunk.UserId,
	}

	version := &models.FileVersion{
		FileID:   fileID,
		Hash:     fileHash,
		Size:     totalSize,
		S3Key:    s3Key,
		DeviceID: firstChunk.DeviceId,
	}

	err = s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Save(file).Error; err != nil {
			return err
		}
		if err := tx.Create(version).Error; err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return status.Errorf(codes.Internal, "failed to save metadata: %v", err)
	}

	return stream.SendAndClose(&proto.FileUploadResponse{
		FileId:  fileID,
		Message: "File uploaded successfully",
	})
}

func (s *FileGatewayService) DownloadFile(req *proto.FileDownloadRequest, stream proto.FileService_DownloadFileServer) error {
	var file models.File
	if err := s.db.Preload("Versions").First(&file, "id = ?", req.FileId).Error; err != nil {
		return status.Errorf(codes.NotFound, "file not found: %v", err)
	}

	if len(file.Versions) == 0 {
		return status.Errorf(codes.NotFound, "no versions found for file")
	}
	version := file.Versions[len(file.Versions)-1]

	reader, err := s.s3Client.DownloadFile(context.Background(), version.S3Key)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to download from S3: %v", err)
	}
	defer reader.Close()

	buffer := make([]byte, chunkSize)
	for {
		n, err := reader.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "failed to read file: %v", err)
		}

		response := &proto.FileDownloadResponse{
			Content: buffer[:n],
		}

		if err := stream.Send(response); err != nil {
			return status.Errorf(codes.Internal, "failed to send chunk: %v", err)
		}
	}

	return nil
}

func (s *FileGatewayService) GetFileMetadata(ctx context.Context, req *proto.FileMetadataRequest) (*proto.FileMetadataResponse, error) {
	var file models.File
	if err := s.db.Preload("Versions").First(&file, "id = ?", req.FileId).Error; err != nil {
		return nil, status.Errorf(codes.NotFound, "file not found: %v", err)
	}

	latestVersion := file.Versions[len(file.Versions)-1]

	return &proto.FileMetadataResponse{
		FileId:      file.ID,
		FileName:    file.Name,
		Size:        file.Size,
		ContentType: file.ContentType,
		CreatedAt:   file.CreatedAt.String(),
		UpdatedAt:   file.UpdatedAt.String(),
		VersionId:   latestVersion.ID,
		OwnerId:     file.OwnerID,
	}, nil
}

func (s *FileGatewayService) ListFiles(ctx context.Context, req *proto.ListFilesRequest) (*proto.ListFilesResponse, error) {
	var files []models.File
	var totalCount int64

	query := s.db.Model(&models.File{}).Where("owner_id = ?", req.UserId)
	if req.FolderPath != "" {
		query = query.Where("path LIKE ?", req.FolderPath+"%")
	}

	if err := query.Count(&totalCount).Error; err != nil {
		return nil, status.Errorf(codes.Internal, "failed to count files: %v", err)
	}

	offset := (req.Page - 1) * req.PageSize
	if err := query.Offset(int(offset)).Limit(int(req.PageSize)).Preload("Versions").Find(&files).Error; err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list files: %v", err)
	}

	response := &proto.ListFilesResponse{
		TotalCount: int32(totalCount),
		Files:      make([]*proto.FileMetadataResponse, len(files)),
	}

	for i, file := range files {
		latestVersion := file.Versions[len(file.Versions)-1]
		response.Files[i] = &proto.FileMetadataResponse{
			FileId:      file.ID,
			FileName:    file.Name,
			Size:        file.Size,
			ContentType: file.ContentType,
			CreatedAt:   file.CreatedAt.String(),
			UpdatedAt:   file.UpdatedAt.String(),
			VersionId:   latestVersion.ID,
			OwnerId:     file.OwnerID,
		}
	}

	return response, nil
}
