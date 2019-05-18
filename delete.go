package main

import (
	"errors"
	"fmt"
	"net/url"
	"sync"

	"github.com/vbauerster/mpb/decor"

	"github.com/vbauerster/mpb"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"go.uber.org/zap"
)

type BucketDeleter struct {
	source      url.URL
	recursive   bool
	versions    bool
	resultsChan chan []*s3.ObjectIdentifier
	count       *mpb.Bar
	wg          sync.WaitGroup
	svc         *s3.S3
	lister      *BucketLister
}

func (deleter *BucketDeleter) deleteObjects() func(item []*s3.ObjectIdentifier) {
	var logger = zap.S()
	return func(item []*s3.ObjectIdentifier) {
		defer deleter.wg.Done()

		deleteInput := s3.DeleteObjectsInput{
			Bucket: aws.String(deleter.source.Host),
			Delete: &s3.Delete{
				Objects: item,
				Quiet:   aws.Bool(true)},
		}

		_, err := deleter.svc.DeleteObjects(&deleteInput)

		deleter.count.IncrBy(len(item))
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

func (deleter *BucketDeleter) deleteAllObjects() {
	deleteObjectsFunc := deleter.deleteObjects()

	var totalObjects int64 = 0
	for item := range deleter.resultsChan {
		deleter.wg.Add(1)
		go deleteObjectsFunc(item)
		totalObjects = totalObjects + int64(len(item))
		deleter.count.SetTotal(totalObjects, false)
	}
	deleter.count.SetTotal(totalObjects, true)
	deleter.wg.Wait()

}

func (deleter *BucketDeleter) delete() {
	var logger = zap.S()

	if deleter.recursive {

		progress := mpb.New()

		deleter.count = progress.AddBar(0,
			mpb.PrependDecorators(
				// simple name decorator
				decor.Name("Files", decor.WC{W: 6, C: decor.DSyncWidth}),
				decor.CountersNoUnit(" %d / %d", decor.WCSyncWidth),
			),
		)

		if deleter.versions {
			go deleter.lister.listObjectVersions(false)
		} else {

			go deleter.lister.listObjects()
		}

		deleter.deleteAllObjects()

	} else {
		//we are only deleting a single object
		//ensure we have more than just the host in the url

		if deleter.source.Path == "" {
			fmt.Println("Must pass an object in the bucket to remove, not just the bucket name")
			logger.Fatal("Must pass an object in the bucket to remove, not just the bucket name")
		}

		if deleter.versions {
			//we want to delete all versions of the object specified

			progress := mpb.New()

			deleter.count = progress.AddBar(0,
				mpb.PrependDecorators(
					// simple name decorator
					decor.Name("Files", decor.WC{W: 6, C: decor.DSyncWidth}),
					decor.CountersNoUnit(" %d / %d", decor.WCSyncWidth),
				),
			)

			go deleter.lister.listObjectVersions(true)

		} else {
			_, err := deleter.svc.DeleteObject(&s3.DeleteObjectInput{
				Bucket: aws.String(deleter.source.Host),
				Key:    aws.String(deleter.source.Path[1:]),
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

}

func NewBucketDeleter(source string, threads int, versions bool, recursive bool, sess *session.Session) (*BucketDeleter, error) {

	sourceURL, err := url.Parse(source)
	if err != nil {
		return nil, err
	}

	if sourceURL.Scheme != "s3" {
		return nil, errors.New("usage: aws s3 ls <S3Uri> ")

	}

	bd := &BucketDeleter{
		source:      *sourceURL,
		wg:          sync.WaitGroup{},
		resultsChan: make(chan []*s3.ObjectIdentifier, threads),
		versions:    versions,
		recursive:   recursive,
	}

	bd.lister, err = NewBucketLister(source, threads, sess)
	bd.lister.resultsChan = bd.resultsChan

	if err != nil {
		return nil, err
	}

	bd.svc, err = checkBucket(sess, sourceURL.Host)
	if err != nil {
		return nil, err
	}

	return bd, nil
}
