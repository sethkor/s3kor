package main

import (
	"errors"
	"fmt"
	"net/url"
	"sync"

	"github.com/aws/aws-sdk-go/aws/awserr"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"go.uber.org/zap"
)

type objectCounter struct {
	count int
	size  int64
}

type BucketLister struct {
	source      url.URL
	resultsChan chan []*s3.ObjectIdentifier
	sizeChan    chan objectCounter
	wg          sync.WaitGroup
	svc         *s3.S3
	threads     int
}

func (lister *BucketLister) processListObjectsVersionsOutput(exactMatchKey string) func(versions []*s3.ObjectVersion, deleters []*s3.DeleteMarkerEntry) {

	if exactMatchKey == "" {
		return func(versions []*s3.ObjectVersion, deleters []*s3.DeleteMarkerEntry) {
			defer lister.wg.Done()
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

			lister.resultsChan <- objectList
		}
	}
	return func(versions []*s3.ObjectVersion, deleters []*s3.DeleteMarkerEntry) {
		defer lister.wg.Done()
		objectList := make([]*s3.ObjectIdentifier, 0, len(versions)+len(deleters))

		for _, item := range versions {
			if *item.Key == exactMatchKey {
				objectList = append(objectList, &s3.ObjectIdentifier{Key: item.Key, VersionId: item.VersionId})
			}
		}

		for _, item := range deleters {
			if *item.Key == exactMatchKey {
				objectList = append(objectList, &s3.ObjectIdentifier{Key: item.Key, VersionId: item.VersionId})
			}
		}

		lister.resultsChan <- objectList
	}

}

func (lister *BucketLister) processListObjectsOutput(withSize bool) func(contents []*s3.Object) {

	//if withSize {
	//	lister.sizeChan = make(chan objectCounter, lister.threads)
	//}
	return func(contents []*s3.Object) {
		defer lister.wg.Done()
		objectList := make([]*s3.ObjectIdentifier, len(contents))

		var fileSizeTotal int64 = 0
		objectPos := 0

		for _, item := range contents {
			objectList[objectPos] = &s3.ObjectIdentifier{Key: item.Key}
			objectPos++
			if withSize {
				fileSizeTotal += *item.Size
			}
		}
		lister.resultsChan <- objectList
		if withSize {
			lister.sizeChan <- objectCounter{
				count: objectPos,
				size:  fileSizeTotal,
			}
		}
	}
}

func (lister *BucketLister) listObjectVersions(exactMatch bool) {
	defer close(lister.resultsChan)
	var logger = zap.S()
	logger.Infof("Listing all object versions and delete markers in bucket: %s", lister.source.RawPath)
	listVersionsInput := s3.ListObjectVersionsInput{
		Bucket: aws.String(lister.source.Host),
	}
	if len(lister.source.Path) > 0 {
		listVersionsInput.Prefix = aws.String(lister.source.Path[1:])
	}

	var numObjects int64 = 0

	var exactMatchKey string
	if exactMatch {
		exactMatchKey = lister.source.Path[1:]
	}
	processListObjectsVersionsOutputFunc := lister.processListObjectsVersionsOutput(exactMatchKey)
	err := lister.svc.ListObjectVersionsPages(&listVersionsInput, func(result *s3.ListObjectVersionsOutput, lastPage bool) bool {

		numObjects = numObjects + int64(len(result.Versions)+len(result.DeleteMarkers))

		if numObjects > 0 {
			lister.wg.Add(1)
			go processListObjectsVersionsOutputFunc(result.Versions, result.DeleteMarkers)
		} //if
		return true
	})

	lister.wg.Wait()

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

func (lister *BucketLister) listObjects(withSize bool) {
	defer close(lister.resultsChan)
	var logger = zap.S()

	listInput := s3.ListObjectsV2Input{
		Bucket: aws.String(lister.source.Host),
	}

	if len(lister.source.Path) > 0 {
		listInput.Prefix = aws.String(lister.source.Path[1:])
	}

	var numObjects int64 = 0
	processListObjectsOutputFunc := lister.processListObjectsOutput(withSize)

	err := lister.svc.ListObjectsV2Pages(&listInput, func(result *s3.ListObjectsV2Output, lastPage bool) bool {
		numObjects = numObjects + int64(len(result.Contents))

		if numObjects > 0 {
			lister.wg.Add(1)
			go processListObjectsOutputFunc(result.Contents)
		} //if
		return true
	})

	lister.wg.Wait()

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

func (lister *BucketLister) printAllObjects() {
	for item := range lister.resultsChan {
		for _, object := range item {
			fmt.Println(*object.Key)
		}

	}
}

func (lister *BucketLister) list(versions bool) {

	if versions {
		go lister.listObjectVersions(false)
	} else {

		go lister.listObjects(false)
	}

	lister.printAllObjects()

}

func NewBucketLister(source string, threads int, sess *session.Session) (*BucketLister, error) {

	sourceURL, err := url.Parse(source)
	if err != nil {
		return nil, err
	}

	if sourceURL.Scheme != "s3" {
		return nil, errors.New("usage: aws s3 ls <S3Uri> ")

	}

	bl := &BucketLister{
		source:  *sourceURL,
		wg:      sync.WaitGroup{},
		threads: threads,
	}

	bl.svc, err = checkBucket(sess, sourceURL.Host)
	if err != nil {
		return nil, err
	}

	return bl, nil
}
