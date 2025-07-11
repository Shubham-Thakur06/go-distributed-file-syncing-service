package main

import (
	"fmt"
	"log"
	"net"

	"github.com/Shubham-Thakur06/go-distributed-file-syncing-service/internal/auth"
	"github.com/Shubham-Thakur06/go-distributed-file-syncing-service/internal/proto"
	"github.com/Shubham-Thakur06/go-distributed-file-syncing-service/internal/utils"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	config, err := utils.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	db := utils.InitDB(config)
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatal("Failed to get underlying DB connection")
	}
	defer sqlDB.Close()

	server := grpc.NewServer()

	authService := auth.NewAuthService(db, config.JWTSecret)
	proto.RegisterAuthServiceServer(server, authService)

	reflection.Register(server)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.AuthServicePort))
	if err != nil {
		log.Fatal("Failed to listen on port", config.AuthServicePort)
	}

	log.Println("Starting Auth Service on port", config.AuthServicePort)
	if err := server.Serve(lis); err != nil {
		log.Fatal("Failed to serve")
	}
}
