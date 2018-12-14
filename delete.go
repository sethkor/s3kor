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

func processListObjectsVersionsOutput(resultsChan chan<- []*s3.ObjectIdentifier, wg *sync.WaitGroup) func(versions []*s3.ObjectVersion, deleters []*s3.DeleteMarkerEntry) {

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

func listObjectVersions(s3URL url.URL, resultsChan chan<- []*s3.ObjectIdentifier, bar *pb.ProgressBar, svc s3.S3) {
	defer close(resultsChan)
	var logger = zap.S()
	logger.Infof("Deleting all object versions and delete markers in bucket: %s", s3URL.RawPath)
	listVersionsInput := s3.ListObjectVersionsInput{
		Bucket: aws.String(s3URL.Host),
	}
	if len(s3URL.Path) > 0 {
		listVersionsInput.Prefix = aws.String(s3URL.Path[1:])
	}

	var wg sync.WaitGroup

	numObjects := 0
	bar.SetTotal(numObjects)
	processListObjectsVersionsOutputFunc := processListObjectsVersionsOutput(resultsChan, &wg)
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

func checkBucket(sess *session.Session, bucket string, autoRegion bool) (svc *s3.S3, err error) {

	var logger = zap.S()

	svc = s3.New(sess, &aws.Config{MaxRetries: aws.Int(30)})

	result, err := svc.GetBucketLocation(&s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	})

	if err != nil {

		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {

			case s3.ErrCodeNoSuchBucket:
				fmt.Println(aerr.Message())
				logger.Fatal(aerr.Message())
			default:
				logger.Fatal(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			logger.Fatal(err.Error())
		}
		return svc, err
	} //if

	if autoRegion {
		svc = s3.New(sess, &aws.Config{MaxRetries: aws.Int(30),
			Region: result.LocationConstraint})
	} else {
		if svc.Config.Region != result.LocationConstraint {
			fmt.Println("Bucket exist in region", *result.LocationConstraint, "which is different to region passed", *svc.Config.Region, ". Please adjust region on the command line our use --auto-region")
			logger.Fatal("Bucket exist in region", *result.LocationConstraint, "which is different to region passed", *svc.Config.Region, ". Please adjust region on the command line our use --auto-region")
		}
	}

	return svc, err
}

func delete(sess *session.Session, path string, autoRegion bool, versions bool, recursive bool) {
	var logger = zap.S()

	s3URL, err := url.Parse(path)

	if err == nil && s3URL.Scheme == "s3" {

		var svc *s3.S3
		svc, err = checkBucket(sess, s3URL.Host, autoRegion)

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

		deleteAllObjects(s3URL.Host, resultsChan, bar, *svc)

		if bar.Total == 0 {
			bar.FinishPrint("No objects found and removed")
		}
		bar.Finish()

	} else {
		fmt.Println("S3 URL passed not formatted correctly")
		logger.Fatal("S3 URL passed not formatted correctly")
	}
}
