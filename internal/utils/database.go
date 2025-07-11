package utils

import (
	"log"

	"github.com/Shubham-Thakur06/go-distributed-file-syncing-service/internal/models"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/plugin/dbresolver"
)

type DBConfig struct {
	Master   *Config
	Replicas []*Config
}

func InitDistributedDB(config *DBConfig) *gorm.DB {
	db, err := gorm.Open(postgres.Open(config.Master.GetDBConnString()), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to master database: %v", err)
	}

	if len(config.Replicas) > 0 {
		replicas := []gorm.Dialector{}
		for _, replica := range config.Replicas {
			replicas = append(replicas, postgres.Open(replica.GetDBConnString()))
		}

		err = db.Use(dbresolver.Register(dbresolver.Config{
			Replicas: replicas,
			Policy:   dbresolver.RandomPolicy{},
		}))
		if err != nil {
			log.Fatalf("Failed to configure database replicas: %v", err)
		}
	}

	// Auto-migrate the models
	err = db.AutoMigrate(&models.User{}, &models.File{}, &models.FileVersion{})
	if err != nil {
		log.Fatalf("Failed to migrate database: %v", err)
	}

	return db
}

func InitDB(config *Config) *gorm.DB {
	return InitDistributedDB(&DBConfig{
		Master: config,
	})
}
