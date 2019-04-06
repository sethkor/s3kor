package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"net/url"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"go.uber.org/zap"
	"gopkg.in/cheggaaa/pb.v1"
)

func processListObjectsVersionsOutput(resultsChan chan<- []*s3.ObjectIdentifier, exactMatchKey string, wg *sync.WaitGroup) func(versions []*s3.ObjectVersion, deleters []*s3.DeleteMarkerEntry) {

	if exactMatchKey == "" {
		return func(versions []*s3.ObjectVersion, deleters []*s3.DeleteMarkerEntry) {
			defer wg.Done()
			objectList := make([]*s3.ObjectIdentifier, len(versions)+len(deleters))
			objectPos := 0

			for _, item := range versions {
				objectList[objectPos] = &s3.ObjectIdentifier{Key: item.Key, VersionId: item.VersionId}
				objectPos++
			}

			for _, item := range deleters {
				objectList[objectPos] = &s3.ObjectIdentifier{Key: item.Key, VersionId: item.VersionId}
				objectPos++
			}

			resultsChan <- objectList
		}
	}
	return func(versions []*s3.ObjectVersion, deleters []*s3.DeleteMarkerEntry) {
		defer wg.Done()
		objectList := make([]*s3.ObjectIdentifier, 0, len(versions)+len(deleters))

		for _, item := range versions {
			if *item.Key == exactMatchKey {
				objectList = append(objectList, &s3.ObjectIdentifier{Key: item.Key, VersionId: item.VersionId})
			}
		}

		for _, item := range deleters {
			if *item.Key == exactMatchKey {
				objectList= append(objectList, &s3.ObjectIdentifier{Key: item.Key, VersionId: item.VersionId})
			}
		}

		resultsChan <- objectList
	}

}

func processListObjectsOutput(resultsChan chan<- []*s3.ObjectIdentifier, wg *sync.WaitGroup) func(contents []*s3.Object) {

	return func(contents []*s3.Object) {
		defer wg.Done()
		objectList := make([]*s3.ObjectIdentifier, len(contents))
		objectPos := 0

		for _, item := range contents {
			objectList[objectPos] = &s3.ObjectIdentifier{Key: item.Key}
			objectPos++
		}
		resultsChan <- objectList
	}
}

func listObjectVersions(s3URL url.URL, resultsChan chan<- []*s3.ObjectIdentifier, exactMatch bool, bar *pb.ProgressBar, svc s3.S3) {
	defer close(resultsChan)
	var logger = zap.S()
	logger.Infof("Listing all object versions and delete markers in bucket: %s", s3URL.RawPath)
	listVersionsInput := s3.ListObjectVersionsInput{
		Bucket: aws.String(s3URL.Host),
	}
	if len(s3URL.Path) > 0 {
		listVersionsInput.Prefix = aws.String(s3URL.Path[1:])
	}

	var wg sync.WaitGroup

	numObjects := 0
	bar.SetTotal(numObjects)

	var exactMatchKey string
	if exactMatch {
		exactMatchKey = s3URL.Path[1:]
	}
	processListObjectsVersionsOutputFunc := processListObjectsVersionsOutput(resultsChan, exactMatchKey, &wg)
	err := svc.ListObjectVersionsPages(&listVersionsInput, func(result *s3.ListObjectVersionsOutput, lastPage bool) bool {

		numObjects = numObjects + len(result.Versions) + len(result.DeleteMarkers)

		if numObjects > 0 {
			bar.SetTotal(numObjects)
			wg.Add(1)
			go processListObjectsVersionsOutputFunc(result.Versions, result.DeleteMarkers)
		} //if
		return true
	})

	wg.Wait()

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				logger.Fatal(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			logger.Fatal(err.Error())
		}
		return
	} //if

}

func listObjects(s3URL url.URL, resultsChan chan<- []*s3.ObjectIdentifier, bar *pb.ProgressBar, svc s3.S3) {
	defer close(resultsChan)
	var logger = zap.S()

	listInput := s3.ListObjectsV2Input{
		Bucket: aws.String(s3URL.Host),
	}

	if len(s3URL.Path) > 0 {
		listInput.Prefix = aws.String(s3URL.Path[1:])
	}

	var wg sync.WaitGroup

	numObjects := 0
	bar.SetTotal(numObjects)
	processListObjectsOutputFunc := processListObjectsOutput(resultsChan, &wg)

	err := svc.ListObjectsV2Pages(&listInput, func(result *s3.ListObjectsV2Output, lastPage bool) bool {
		numObjects = numObjects + len(result.Contents)

		if numObjects > 0 {
			bar.SetTotal(numObjects)
			wg.Add(1)
			go processListObjectsOutputFunc(result.Contents)
		} //if
		return true
	})

	wg.Wait()

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				logger.Fatal(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			logger.Fatal(err.Error())
		}
		return
	} //if

}

func printAllObjects(deleteBucket string, resultsChan <-chan []*s3.ObjectIdentifier, bar *pb.ProgressBar, svc s3.S3) {
	for item := range resultsChan {
		for _, object := range item {
			fmt.Println(*object.Key)
		}

	}
}

func list(sess *session.Session, path string, autoRegion bool, versions bool) {
	var logger = zap.S()

	s3URL, err := url.Parse(path)

	if err == nil && s3URL.Scheme == "s3" {

		var svc *s3.S3
		svc, err = checkBucket(sess, s3URL.Host, autoRegion)

		threads := 50
		//make a channel for processing
		resultsChan := make(chan []*s3.ObjectIdentifier, threads)

		bar := pb.New(0)
		//bar := pb.StartNew(0)
		bar.ShowBar = false
		bar.NotPrint = true
		if versions {
			go listObjectVersions(*s3URL, resultsChan, false, bar, *svc)
		} else {

			go listObjects(*s3URL, resultsChan, bar, *svc)
		}

		printAllObjects(s3URL.Host, resultsChan, bar, *svc)

		if bar.Total == 0 {
			bar.FinishPrint("No objects found and removed")
		}
		//bar.Finish()

		fmt.Printf("%d items found\n", bar.Total)

	} else {
		fmt.Println("S3 URL passed not formatted correctly")
		logger.Fatal("S3 URL passed not formatted correctly")
	}
}
