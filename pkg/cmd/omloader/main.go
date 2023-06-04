package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/herkolategan/omloader"
	"go.uber.org/zap"
)

func run() error {
	logger, err := zap.NewProduction()
	if err != nil {
		return err
	}

	config := omloader.GoogleWatcherConfig{
		TopicID:        "omloader",
		SubscriptionID: "omloader-sub",
	}

	httpSenderConfig := omloader.HttpSenderConfig{}

	flag.StringVar(&config.ProjectID, "project.id", "", "Google Cloud project ID.")
	flag.StringVar(&config.BucketName, "bucket.name", "", "Google Cloud Storage bucket name.")
	flag.StringVar(&config.BucketPath, "bucket.path", "metrics/incoming", "Google Cloud Storage bucket path.")
	flag.StringVar(&httpSenderConfig.URL, "prom.url", "http://localhost:9202/write", "Prometheus adapter remote write URL.")
	flag.Parse()

	logger.Info("config", zap.Any("config", config))

	sender := omloader.NewHttpSender(httpSenderConfig)
	processor := omloader.NewProcessor(logger, sender)

	watcher, err := omloader.NewGoogleWatcher(logger, processor, config)
	if err != nil {
		return err
	}
	return watcher.Run()
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
