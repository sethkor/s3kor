package main

import (
	"fmt"
	"net/url"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"go.uber.org/zap"
	"gopkg.in/cheggaaa/pb.v1"
)

func deleteObjects(deleteBucket string, bar *pb.ProgressBar, wg *sync.WaitGroup, svc s3.S3) func(item []*s3.ObjectIdentifier) {
	var logger = zap.S()
	return func(item []*s3.ObjectIdentifier) {
		defer wg.Done()

		deleteInput := s3.DeleteObjectsInput{
			Bucket: aws.String(deleteBucket),
			Delete: &s3.Delete{
				Objects: item,
				Quiet:   aws.Bool(true)},
		}

		_, err := svc.DeleteObjects(&deleteInput)
		bar.Add(len(deleteInput.Delete.Objects))
		if err != nil {
			if aerr, ok := err.(awserr.RequestFailure); ok {
				switch aerr.StatusCode() {

				default:
					logger.Error(*deleteInput.Delete)
					logger.Error(aerr.Error())
				} //default
			} else {
				// Print the error, cast err to awserr.Error to get the Code and
				// Message from an error.
				logger.Error(err.Error())
			} //else
			return
		} //if
	}
}

func deleteAllObjects(deleteBucket string, resultsChan <-chan []*s3.ObjectIdentifier, bar *pb.ProgressBar, svc s3.S3) {
	var wg sync.WaitGroup
	deleteObjectsFunc := deleteObjects(deleteBucket, bar, &wg, svc)
	for item := range resultsChan {
		wg.Add(1)
		go deleteObjectsFunc(item)
	}
	wg.Wait()

}

func delete(sess *session.Session, path string, versions bool, recursive bool) {
	var logger = zap.S()

	s3URL, err := url.Parse(path)

	if err == nil && s3URL.Scheme == "s3" {

		var svc *s3.S3
		svc, err = checkBucket(sess, s3URL.Host)

		if recursive {
			threads := 50
			//make a channel for processing
			resultsChan := make(chan []*s3.ObjectIdentifier, threads)

			bar := pb.StartNew(0)
			bar.ShowBar = true
			if versions {
				go listObjectVersions(*s3URL, resultsChan, false, bar, *svc)
			} else {

				go listObjects(*s3URL, resultsChan, bar, *svc)
			}

			deleteAllObjects(s3URL.Host, resultsChan, bar, *svc)

			if bar.Total == 0 {
				bar.FinishPrint("No objects found and removed")
			}
			bar.Finish()
		} else {
			//we are only deleting a single object
			//ensure we have more than just the host in the url

			if s3URL.Path == "" {
				fmt.Println("Must pass an object in the bucket to remove, not just the bucket name")
				logger.Fatal("Must pass an object in the bucket to remove, not just the bucket name")
			}

			if versions {
				//we want to delete all versions of the object specified

				//make a channel for processing
				threads := 50
				resultsChan := make(chan []*s3.ObjectIdentifier, threads)

				bar := pb.StartNew(0)
				bar.ShowBar = true

				go listObjectVersions(*s3URL, resultsChan, true, bar, *svc)

			} else {
				_, err = svc.DeleteObject(&s3.DeleteObjectInput{
					Bucket: aws.String(s3URL.Host),
					Key:    aws.String(s3URL.Path[1:]),
				})

				if err != nil {
					if aerr, ok := err.(awserr.RequestFailure); ok {
						switch aerr.StatusCode() {

						default:
							logger.Error(aerr.Error())
						} //default
					} else {
						// Print the error, cast err to awserr.Error to get the Code and
						// Message from an error.
						logger.Error(err.Error())
					} //else
					return
				} //if
			}
		}

	} else {
		fmt.Println("S3 URL passed not formatted correctly")
		logger.Fatal("S3 URL passed not formatted correctly")
	}
}
