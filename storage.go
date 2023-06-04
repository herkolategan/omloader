package omloader

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"

	"cloud.google.com/go/storage"
)

type Storage interface {
	Read(path string) (io.ReadCloser, error)
	Move(src, dst string) error
	Delete(path string) error
	io.Closer
}

type GoogleStorage interface {
	Storage
	RegisterFinalizeNotification(bucketName, projectID, topicID string) error
}

type googleStorage struct {
	client *storage.Client
	ctx    context.Context
}

func NewGoogleStorage() (GoogleStorage, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)

	if err != nil {
		return nil, err
	}

	return &googleStorage{
		client: client,
		ctx:    ctx,
	}, nil
}

func (s *googleStorage) Close() error {
	return s.client.Close()
}

func (s *googleStorage) Read(path string) (io.ReadCloser, error) {
	bucketName, objectName, err := parseURI(path)
	if err != nil {
		return nil, err
	}

	fmt.Println(bucketName, objectName)

	bucket := s.client.Bucket(bucketName)
	obj := bucket.Object(objectName)
	return obj.NewReader(s.ctx)
}

func (s *googleStorage) Move(src, dst string) error {
	srcBucketName, srcObjectName, err := parseURI(src)
	if err != nil {
		return err
	}

	dstBucketName, dstObjectName, err := parseURI(dst)
	if err != nil {
		return err
	}

	srcBucket := s.client.Bucket(srcBucketName)
	srcObject := srcBucket.Object(srcObjectName)

	dstBucket := s.client.Bucket(dstBucketName)
	dstObject := dstBucket.Object(dstObjectName)

	if _, err := dstObject.CopierFrom(srcObject).Run(s.ctx); err != nil {
		return err
	}

	if err := srcObject.Delete(s.ctx); err != nil {
		return err
	}

	return nil
}

func (s *googleStorage) Delete(path string) error {
	bucketName, objectName, err := parseURI(path)
	if err != nil {
		return err
	}

	bucket := s.client.Bucket(bucketName)
	object := bucket.Object(objectName)

	if err := object.Delete(s.ctx); err != nil {
		return err
	}

	return nil
}

func (s *googleStorage) RegisterFinalizeNotification(bucketName, projectID, topicID string) error {
	bucket := s.client.Bucket(bucketName)
	notifications, err := bucket.Notifications(s.ctx)
	if err != nil {
		return err
	}

	// Check if notification already exists.
	for _, v := range notifications {
		if v.TopicID == topicID && v.TopicProjectID == projectID {
			return nil
		}
	}

	_, err = bucket.AddNotification(s.ctx, &storage.Notification{
		TopicProjectID: projectID,
		TopicID:        topicID,
		EventTypes:     []string{"OBJECT_FINALIZE"},
		PayloadFormat:  storage.JSONPayload,
	})
	return err
}

// parseURI parses a GCS URI into a bucket name and object name.
func parseURI(uri string) (string, string, error) {
	re := regexp.MustCompile(`gs://(.*?)/(.*)`)
	matches := re.FindStringSubmatch(uri)
	if len(matches) != 3 {
		return "", "", fmt.Errorf("invalid GCS URI: %s", uri)
	}
	return matches[1], matches[2], nil
}

type fileStorage struct{}

func NewFileStorage() Storage {
	return &fileStorage{}
}

func (s *fileStorage) Close() error {
	return nil
}

func (s *fileStorage) Read(path string) (io.ReadCloser, error) {
	return os.Open(path)
}

func (s *fileStorage) Move(src, dst string) error {
	return os.Rename(src, dst)
}

func (s *fileStorage) Delete(path string) error {
	return os.Remove(path)
}
