package omloader

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"cloud.google.com/go/pubsub"
	"github.com/googleapis/google-cloudevents-go/cloud/storagedata"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
)

type GoogleWatcherConfig struct {
	ProjectID      string
	TopicID        string
	BucketName     string
	BucketPath     string
	SubscriptionID string
}

type googleWatcher struct {
	GoogleWatcherConfig
	processor     Processor
	googleStorage GoogleStorage
	ctx           context.Context
	logger        *zap.Logger
}

func NewGoogleWatcher(logger *zap.Logger, processor Processor, config GoogleWatcherConfig) (*googleWatcher, error) {
	storage, err := NewGoogleStorage()
	if err != nil {
		return nil, err
	}

	return &googleWatcher{
		GoogleWatcherConfig: config,
		processor:           processor,
		googleStorage:       storage,
		logger:              logger,
		ctx:                 context.Background(),
	}, nil
}

func (w *googleWatcher) Run() error {
	w.logger.Info("Starting watcher...")
	// Set up Google Cloud Pub/Sub client.
	pubsubClient, err := pubsub.NewClient(w.ctx, w.ProjectID)
	if err != nil {
		return errors.Wrap(err, "failed to create pubsub client")
	}

	// Delete Google Cloud Pub/Sub topic if it already exists.
	topic := pubsubClient.Topic(w.TopicID)
	exists, err := topic.Exists(w.ctx)
	if err != nil {
		return errors.Wrap(err, "failed to check if pubsub topic exists")
	}
	if !exists {
		// Set up Google Cloud Pub/Sub topic.
		topic, err = pubsubClient.CreateTopic(w.ctx, w.TopicID)
		if err != nil {
			return errors.Wrap(err, "failed to create pubsub topic")
		}
	}

	// Set up Google Cloud Storage bucket notification.
	err = w.googleStorage.RegisterFinalizeNotification(w.BucketName, w.ProjectID, w.TopicID)
	if err != nil {
		return errors.Wrap(err, "failed to register bucket notification")
	}

	sub := pubsubClient.Subscription(w.SubscriptionID)
	exists, err = sub.Exists(w.ctx)
	if err != nil {
		return errors.Wrap(err, "failed to check if pubsub subscription exists")
	}
	if !exists {
		// Subscribe to Google Cloud Pub/Sub subscription.
		sub, err = pubsubClient.CreateSubscription(w.ctx, w.SubscriptionID, pubsub.SubscriptionConfig{
			Topic: topic,
		})
		if err != nil {
			return errors.Wrap(err, "failed to create pubsub subscription")
		}
	}

	subCtx, subCancel := context.WithCancel(w.ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = sub.Receive(subCtx, func(ctx context.Context, msg *pubsub.Message) {
			defer msg.Ack()

			var data storagedata.StorageObjectData
			if err := protojson.Unmarshal(msg.Data, &data); err != nil {
				w.logger.Error("Failed to unmarshal event data", zap.Error(err))
				return
			}

			// Only process files in the configured bucket path with the .om extension.
			dir := path.Dir(data.Name)
			if dir != w.BucketPath {
				return
			}
			if !strings.HasSuffix(data.Name, ".om") {
				return
			}

			w.logger.Info("Received bucket event", zap.String("name", data.Name))
			uri := fmt.Sprintf("gs://%s/%s", data.Bucket, data.Name)
			reader, err := w.googleStorage.Read(uri)
			if err != nil {
				w.logger.Error("Failed to read file", zap.String("uri", uri), zap.Error(err))
				return
			}
			defer func() {
				err := reader.Close()
				if err != nil {
					w.logger.Error("Failed to close reader", zap.String("uri", uri), zap.Error(err))
				}
			}()

			err = w.processor.Process(reader)
			if err != nil {
				w.logger.Error("Failed to process file", zap.String("uri", uri), zap.Error(err))
				return
			}

			w.logger.Info("Processed file", zap.String("uri", uri))

			if err := w.googleStorage.Move(uri, w.toProcessedPath(data.Name)); err != nil {
				w.logger.Error("Failed to move file to processed", zap.String("uri", uri), zap.Error(err))
				return
			}
		})
		if err != nil {
			fmt.Printf("Failed to receive message: %v\n", err)
			subCancel()
			return
		}
	}()

	w.logger.Info("Watcher started", zap.String("subscription", sub.String()))
	stop := make(chan os.Signal, 1)
	defer close(stop)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	select {
	case <-stop:
		w.logger.Info("Received SIGTERM, exiting gracefully...")
	case <-subCtx.Done():
		w.logger.Info("Received cancelation, exiting gracefully...")
	}
	subCancel()
	wg.Wait()

	// TODO(herko): do not delete topic and subscription
	/*
		if err := sub.Delete(w.ctx); err != nil {
			return err
		}
		if err := topic.Delete(w.ctx); err != nil {
			return err
		}
	*/

	return nil
}

func (w *googleWatcher) toProcessedPath(path string) string {
	return fmt.Sprintf("gs://%s/%s/processed/%s", w.BucketName, w.BucketPath, filepath.Base(path))
}
