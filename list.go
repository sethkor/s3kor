package main

import (
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"go.uber.org/zap"
	"gopkg.in/cheggaaa/pb.v1"
)

func printAllObjects(deleteBucket string, resultsChan <-chan []*s3.ObjectIdentifier, bar *pb.ProgressBar, svc s3.S3) {
	for item := range resultsChan {
		for _, object := range item {
			fmt.Println(object.GoString())
		}

	}
}

func list(sess *session.Session, path string, versions bool) {
	var logger = zap.S()

	s3URL, err := url.Parse(path)

	if err == nil && s3URL.Scheme == "s3" {

		svc := s3.New(sess, &aws.Config{MaxRetries: aws.Int(30)})

		threads := 50
		//make a channel for processing
		resultsChan := make(chan []*s3.ObjectIdentifier, threads)

		bar := pb.StartNew(0)
		bar.ShowBar = false
		if versions {
			go listObjectVersions(*s3URL, resultsChan, bar, *svc)
		} else {

			go listObjects(*s3URL, resultsChan, bar, *svc)
		}

		printAllObjects(s3URL.Host, resultsChan, bar, *svc)

		if bar.Total == 0 {
			bar.FinishPrint("No objects found and removed")
		}
		bar.Finish()

	} else {
		fmt.Println("S3 URL passed not formatted correctly")
		logger.Fatal("S3 URL passed not formatted correctly")
	}
}
