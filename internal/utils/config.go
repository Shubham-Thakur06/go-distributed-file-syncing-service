package utils

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	// Database
	DBHost     string
	DBPort     int
	DBUser     string
	DBPassword string
	DBName     string

	// AWS
	AWSRegion          string
	AWSAccessKeyID     string
	AWSSecretAccessKey string
	AWSBucketName      string

	// Service
	JWTSecret          string
	AuthServicePort    int
	GatewayServicePort int
}

func LoadConfig() (*Config, error) {
	_ = godotenv.Load()

	config := &Config{}

	// Database configuration
	config.DBHost = getEnvString("DB_HOST", "localhost")
	config.DBPort = getEnvInt("DB_PORT", 5432)
	config.DBUser = getEnvString("DB_USER", "postgres")
	config.DBPassword = getEnvString("DB_PASSWORD", "")
	config.DBName = getEnvString("DB_NAME", "file_sync_db")

	// AWS configuration
	config.AWSRegion = getEnvString("AWS_REGION", "us-east-1")
	config.AWSAccessKeyID = getEnvString("AWS_ACCESS_KEY_ID", "")
	config.AWSSecretAccessKey = getEnvString("AWS_SECRET_ACCESS_KEY", "")
	config.AWSBucketName = getEnvString("AWS_BUCKET_NAME", "")

	// Service configuration
	config.JWTSecret = getEnvString("JWT_SECRET", "")
	config.AuthServicePort = getEnvInt("AUTH_SERVICE_PORT", 50051)

	if err := config.validate(); err != nil {
		return nil, err
	}

	return config, nil
}

// Helper functions for environment variables
func getEnvString(key string, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value, exists := os.LookupEnv(key); exists {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value, exists := os.LookupEnv(key); exists {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}

func (c *Config) validate() error {
	// Validate required fields
	if c.JWTSecret == "" {
		return fmt.Errorf("JWT_SECRET is required")
	}

	if c.AWSAccessKeyID == "" || c.AWSSecretAccessKey == "" || c.AWSBucketName == "" {
		return fmt.Errorf("AWS credentials and bucket name are required")
	}

	if c.DBPassword == "" {
		return fmt.Errorf("DB_PASSWORD is required")
	}

	return nil
}

// GetDBConnString returns the PostgreSQL connection string
func (c *Config) GetDBConnString() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		c.DBHost, c.DBPort, c.DBUser, c.DBPassword, c.DBName)
}
