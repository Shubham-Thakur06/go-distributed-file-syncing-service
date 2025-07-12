package main

import (
	"fmt"
	"log"
	"net"

	"github.com/Shubham-Thakur06/go-distributed-file-syncing-service/internal/proto"
	"github.com/Shubham-Thakur06/go-distributed-file-syncing-service/internal/sync"
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

	kafka, err := utils.NewKafkaClient(config.KafkaBrokers)
	if err != nil {
		log.Fatalf("Failed to initialize Kafka client: %v", err)
	}
	defer kafka.Close()

	server := grpc.NewServer()

	syncService := sync.NewSyncService(db, kafka)
	proto.RegisterSyncServiceServer(server, syncService)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.SyncServicePort))
	if err != nil {
		log.Fatalf("Failed to listen on port %d: %v", config.SyncServicePort, err)
	}

	log.Println("Starting Sync service on port", config.SyncServicePort)
	if err := server.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
