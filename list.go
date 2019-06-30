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

// objectCounter stores the total object count ans sum of object sizes returned in a ListObjects page.  This is useful
// for things that track prgress such as a progress bar
type objectCounter struct {
	count int
	size  int64
}

// BucketLister stores everything we need to list a bucket, be it for ls output or processing for a copy or remove
// operation
type BucketLister struct {
	source   url.URL
	objects  chan []*s3.Object
	versions chan []*s3.ObjectIdentifier
	sizeChan chan objectCounter
	wg       sync.WaitGroup
	svc      *s3.S3
	threads  int
}

// Process the output of a list object versionsoperation.  Stores the srcObjects found in a channel of Object Identifiers.
// delete markers which are really srcObjects themselves are also processed and stored on the channel.  For exact match
// operations we filter the result to llook for exact matches.
func (bl *BucketLister) processListObjectsVersionsOutput(exactMatchKey string) func(versions []*s3.ObjectVersion, deleters []*s3.DeleteMarkerEntry) {

	if exactMatchKey == "" {
		return func(versions []*s3.ObjectVersion, deleters []*s3.DeleteMarkerEntry) {
			defer bl.wg.Done()
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

			bl.versions <- objectList
		}
	}
	return func(versions []*s3.ObjectVersion, deleters []*s3.DeleteMarkerEntry) {
		defer bl.wg.Done()
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
		bl.versions <- objectList
	}
}

// Process the output of a list object operation.  Stores the srcObjects found in a channel of Object Identifiers.  Also
// optionally stores size and count of cobjects in a seperate channel
func (bl *BucketLister) processListObjectsOutput(withSize bool) func(contents []*s3.Object) {

	return func(contents []*s3.Object) {
		defer bl.wg.Done()

		bl.objects <- contents

		if withSize {

			var fileSizeTotal int64
			objectPos := 0

			for _, item := range contents {
				objectPos++
				if withSize {
					fileSizeTotal += *item.Size
				}
			}

			bl.sizeChan <- objectCounter{
				count: objectPos,
				size:  fileSizeTotal,
			}
		}
	}
}

// Lists srcObjects and their versions in a bucket
func (bl *BucketLister) listObjectVersions(exactMatch bool) {
	defer close(bl.versions)
	var logger = zap.S()
	logger.Infof("Listing all object versions and delete markers in bucket: %s", bl.source.RawPath)
	listVersionsInput := s3.ListObjectVersionsInput{
		Bucket: aws.String(bl.source.Host),
	}
	if len(bl.source.Path) > 0 {
		listVersionsInput.Prefix = aws.String(bl.source.Path[1:])
	}

	var exactMatchKey string
	if exactMatch {
		exactMatchKey = bl.source.Path[1:]
	}
	processListObjectsVersionsOutputFunc := bl.processListObjectsVersionsOutput(exactMatchKey)

	err := bl.svc.ListObjectVersionsPages(&listVersionsInput, func(result *s3.ListObjectVersionsOutput, lastPage bool) bool {

		if len(result.Versions)+len(result.DeleteMarkers) > 0 {
			bl.wg.Add(1)
			go processListObjectsVersionsOutputFunc(result.Versions, result.DeleteMarkers)
		}
		return true
	})

	bl.wg.Wait()

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
	}
}

// ListObjects lists srcObjects in a bucket
func (bl *BucketLister) ListObjects(withSize bool) {
	defer close(bl.objects)
	var logger = zap.S()

	listInput := s3.ListObjectsV2Input{
		Bucket: aws.String(bl.source.Host),
	}

	if len(bl.source.Path) > 0 {
		listInput.Prefix = aws.String(bl.source.Path[1:])
	}

	processListObjectsOutputFunc := bl.processListObjectsOutput(withSize)

	err := bl.svc.ListObjectsV2Pages(&listInput, func(result *s3.ListObjectsV2Output, lastPage bool) bool {

		if len(result.Contents) > 0 {
			bl.wg.Add(1)
			go processListObjectsOutputFunc(result.Contents)
		}
		return true
	})

	bl.wg.Wait()

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
	}
}

// Prints objets found in the list operation
func (bl *BucketLister) printAllObjects(versions bool) {

	if versions {
		for item := range bl.versions {
			for _, object := range item {
				fmt.Println(*object.VersionId + " " + *object.Key)
			}
		}
	} else {
		for item := range bl.objects {
			for _, object := range item {
				fmt.Println(*object.Key)
			}
		}
	}

}

// List srcObjects for a bucket whose details are stored in the srcLister receiver.  Can list versions too
func (bl *BucketLister) List(versions bool) {

	if versions {
		bl.versions = make(chan []*s3.ObjectIdentifier, bl.threads)
		go bl.listObjectVersions(false)
		close(bl.objects)
	} else {
		bl.objects = make(chan []*s3.Object, bl.threads)
		go bl.ListObjects(false)
		close(bl.versions)
	}
	bl.printAllObjects(versions)
}

func initBucketLister(source string, threads int) (*BucketLister, error) {
	sourceURL, err := url.Parse(source)
	if err != nil {
		return nil, err
	}

	if sourceURL.Scheme != "s3" {
		return nil, errors.New("usage: aws s3 ls <S3Uri> ")

	}

	//construct a new structure.  Initialize resultsChan and sizechan even though they may be overridden
	bl := &BucketLister{
		source:   *sourceURL,
		wg:       sync.WaitGroup{},
		objects:  make(chan []*s3.Object),
		versions: make(chan []*s3.ObjectIdentifier),
		sizeChan: make(chan objectCounter, threads),
		threads:  threads,
	}
	return bl, nil
}

// NewBucketLister creates a new BucketLister struct initialized with all variables needed to list a bucket
func NewBucketLister(source string, threads int, sess *session.Session) (*BucketLister, error) {

	bl, err := initBucketLister(source, threads)

	if err == nil {
		bl.svc, err = checkBucket(sess, bl.source.Host, nil)
	}

	return bl, err
}

// NewBucketListerWithSvc creates a new BucketLister struct initialized with all variables needed to list a bucket
func NewBucketListerWithSvc(source string, threads int, svc *s3.S3) (*BucketLister, error) {

	bl, err := initBucketLister(source, threads)

	if err == nil {
		bl.svc = svc
	}

	return bl, err
}
