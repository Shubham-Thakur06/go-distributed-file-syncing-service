package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/Shubham-Thakur06/go-distributed-file-syncing-service/internal/gateway"
	"github.com/Shubham-Thakur06/go-distributed-file-syncing-service/internal/proto"
	"github.com/Shubham-Thakur06/go-distributed-file-syncing-service/internal/utils"

	"google.golang.org/grpc"
)

func main() {
	config, err := utils.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	db := utils.InitDB(config)
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatalf("Failed to get underlying DB connection: %v", err)
	}
	defer sqlDB.Close()

	s3Client, err := utils.NewS3Client(context.Background(), config.AWSRegion, config.AWSBucketName)
	if err != nil {
		log.Fatalf("Failed to initialize S3 client: %v", err)
	}

	server := grpc.NewServer()

	fileService := gateway.NewFileGatewayService(db, s3Client)
	proto.RegisterFileServiceServer(server, fileService)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.GatewayServicePort))
	if err != nil {
		log.Fatalf("Failed to listen on port %d: %v", config.GatewayServicePort, err)
	}

	log.Println("Starting Gateway Service on port ", config.GatewayServicePort)
	if err := server.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
