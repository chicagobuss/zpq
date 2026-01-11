package s3

import (
	"fmt"
	"math/rand"
	"time"
)

type Part struct {
	PartNumber int
	ETag       string
}

// Writer is a simplified S3 client interface
type Writer interface {
	InitMultipart(key string) (string, error)
	UploadPart(uploadID string, partNumber int, data []byte) (string, error)
	CompleteMultipart(uploadID string, parts []Part) error
	PutObject(key string, data []byte) error
}

// MockWriter simulates S3 behavior with latency
type MockWriter struct {
	Bucket string
	Region string
}

func NewMockWriter(bucket, region string) *MockWriter {
	return &MockWriter{Bucket: bucket, Region: region}
}

func (w *MockWriter) InitMultipart(key string) (string, error) {
	// Simulate latency
	time.Sleep(50 * time.Millisecond)
	uploadID := fmt.Sprintf("upload_id_%d", rand.Int63())
	fmt.Printf("[S3] InitMultipart: %s/%s -> %s\n", w.Bucket, key, uploadID)
	return uploadID, nil
}

func (w *MockWriter) UploadPart(uploadID string, partNumber int, data []byte) (string, error) {
	// Simulate upload time (10MB/s)
	sizeMB := float64(len(data)) / (1024 * 1024)
	latency := time.Duration(sizeMB * 100) * time.Millisecond
	time.Sleep(latency)
	
	etag := fmt.Sprintf("etag_%d", partNumber)
	fmt.Printf("[S3] UploadPart: %s (Part %d, %d bytes) -> %s\n", uploadID, partNumber, len(data), etag)
	return etag, nil
}

func (w *MockWriter) CompleteMultipart(uploadID string, parts []Part) error {
	time.Sleep(100 * time.Millisecond)
	fmt.Printf("[S3] CompleteMultipart: %s (%d parts)\n", uploadID, len(parts))
	return nil
}

func (w *MockWriter) PutObject(key string, data []byte) error {
	time.Sleep(100 * time.Millisecond)
	fmt.Printf("[S3] PutObject: %s/%s (%d bytes)\n", w.Bucket, key, len(data))
	return nil
}
