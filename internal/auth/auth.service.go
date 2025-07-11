package auth

import (
	"context"
	"errors"

	"golang.org/x/crypto/bcrypt"

	"github.com/Shubham-Thakur06/go-distributed-file-syncing-service/internal/middleware"
	"github.com/Shubham-Thakur06/go-distributed-file-syncing-service/internal/models"
	"github.com/Shubham-Thakur06/go-distributed-file-syncing-service/internal/proto"

	"gorm.io/gorm"
)

type AuthService struct {
	proto.UnimplementedAuthServiceServer
	db        *gorm.DB
	jwtSecret string
}

func NewAuthService(db *gorm.DB, jwtSecret string) *AuthService {
	return &AuthService{
		db:        db,
		jwtSecret: jwtSecret,
	}
}

func (s *AuthService) SignUp(ctx context.Context, req *proto.SignUpRequest) (*proto.AuthResponse, error) {
	var existingUser models.User
	result := s.db.Where("email = ?", req.Email).First(&existingUser)
	if result.Error == nil {
		return nil, errors.New("user already exists")
	} else if !errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return nil, result.Error
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}

	user := &models.User{
		Email:    req.Email,
		Username: req.Username,
		Password: string(hashedPassword),
	}

	if err := s.db.Create(user).Error; err != nil {
		return nil, err
	}

	token, err := middleware.GenerateToken(user.ID, s.jwtSecret)
	if err != nil {
		return nil, err
	}

	return &proto.AuthResponse{
		Token:   token,
		UserId:  user.ID,
		Message: "User created successfully",
	}, nil
}

func (s *AuthService) SignIn(ctx context.Context, req *proto.SignInRequest) (*proto.AuthResponse, error) {
	var user models.User
	if err := s.db.Where("email = ?", req.Email).First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("invalid credentials")
		}
		return nil, err
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(req.Password)); err != nil {
		return nil, errors.New("invalid credentials")
	}

	token, err := middleware.GenerateToken(user.ID, s.jwtSecret)
	if err != nil {
		return nil, err
	}

	return &proto.AuthResponse{
		Token:   token,
		UserId:  user.ID,
		Message: "Login successful",
	}, nil
}

func (s *AuthService) ValidateToken(ctx context.Context, req *proto.ValidateTokenRequest) (*proto.ValidateTokenResponse, error) {
	claims, err := middleware.ValidateToken(req.Token, s.jwtSecret)
	if err != nil {
		return &proto.ValidateTokenResponse{
			Valid:  false,
			UserId: "",
		}, nil
	}

	return &proto.ValidateTokenResponse{
		Valid:  true,
		UserId: claims.UserID,
	}, nil
}
