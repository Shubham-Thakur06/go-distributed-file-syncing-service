package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type File struct {
	ID          string        `gorm:"primaryKey;type:uuid" json:"id"`
	Name        string        `gorm:"not null" json:"name"`
	Path        string        `gorm:"not null" json:"path"`
	Size        int64         `json:"size"`
	ContentType string        `json:"content_type"`
	OwnerID     string        `gorm:"type:uuid;not null" json:"owner_id"`
	Owner       User          `gorm:"foreignKey:OwnerID" json:"owner"`
	Versions    []FileVersion `gorm:"foreignKey:FileID" json:"versions"`
	CreatedAt   time.Time     `json:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
}

type FileVersion struct {
	ID         string    `gorm:"primaryKey;type:uuid" json:"id"`
	FileID     string    `gorm:"type:uuid;not null" json:"file_id"`
	File       File      `gorm:"foreignKey:FileID" json:"-"`
	VersionNum int       `gorm:"not null" json:"version_num"`
	Hash       string    `gorm:"not null" json:"hash"`
	Size       int64     `json:"size"`
	S3Key      string    `gorm:"not null" json:"s3_key"`
	DeviceID   string    `json:"device_id"`
	CreatedAt  time.Time `json:"created_at"`
}

func (f *File) BeforeCreate(tx *gorm.DB) error {
	if f.ID == "" {
		f.ID = uuid.New().String()
	}
	return nil
}

func (fv *FileVersion) BeforeCreate(tx *gorm.DB) error {
	if fv.ID == "" {
		fv.ID = uuid.New().String()
	}
	return nil
}
