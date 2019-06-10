package main

import (
	"errors"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"go.uber.org/zap"
)

// BucketDeleter stores everything we need to delete objects in a bucket
type BucketDeleter struct {
	source    url.URL
	quiet     bool
	recursive bool
	//versions    bool
	total    int64
	objects  chan []*s3.Object
	versions chan []*s3.ObjectIdentifier
	wg       sync.WaitGroup
	svc      *s3.S3
	lister   *BucketLister
}

func (bd *BucketDeleter) deleteObjects() func(item []*s3.ObjectIdentifier) {
	var logger = zap.S()
	return func(item []*s3.ObjectIdentifier) {
		defer bd.wg.Done()

		deleteInput := s3.DeleteObjectsInput{
			Bucket: aws.String(bd.source.Host),
			Delete: &s3.Delete{
				Objects: item,
				Quiet:   aws.Bool(true)},
		}

		_, err := bd.svc.DeleteObjects(&deleteInput)

		atomic.AddInt64(&bd.total, int64((len(item))))
		if !bd.quiet {
			fmt.Printf("\r%d", bd.total)
		}

		if err != nil {
			if aerr, ok := err.(awserr.RequestFailure); ok {
				switch aerr.StatusCode() {

				default:
					logger.Error(*deleteInput.Delete)
					logger.Error(aerr.Error())
				}
			} else {
				// Print the error, cast err to awserr.Error to get the Code and
				// Message from an error.
				logger.Error(err.Error())
			}
			return
		}
	}
}

func (bd *BucketDeleter) deleteAllObjects(versions bool) {
	deleteObjectsFunc := bd.deleteObjects()
	if !bd.quiet {
		fmt.Printf("0")
	}

	if versions {
		for item := range bd.versions {
			bd.wg.Add(1)
			go deleteObjectsFunc(item)
		}
	} else {
		for item := range bd.objects {

			//convert from objects to object identifiers
			identifiers := make([]*s3.ObjectIdentifier, len(item))
			for pos, object := range item {
				identifiers[pos] = &s3.ObjectIdentifier{
					Key: object.Key,
				}
			}

			bd.wg.Add(1)
			go deleteObjectsFunc(identifiers)
		}
	}

	bd.wg.Wait()
	if !bd.quiet {
		fmt.Printf("\n")
	}
}

func (bd *BucketDeleter) delete(versions bool) {
	var logger = zap.S()

	if bd.recursive {

		if versions {
			go bd.lister.listObjectVersions(false)
		} else {

			go bd.lister.ListObjects(false)
		}

		bd.deleteAllObjects(versions)

	} else {
		// we are only deleting a single object
		// ensure we have more than just the host in the url

		if bd.source.Path == "" {
			fmt.Println("Must pass an object in the bucket to remove, not just the bucket name")
			logger.Fatal("Must pass an object in the bucket to remove, not just the bucket name")
		}

		if versions {
			// we want to delete all versions of the object specified

			go bd.lister.listObjectVersions(true)

		} else {
			_, err := bd.svc.DeleteObject(&s3.DeleteObjectInput{
				Bucket: aws.String(bd.source.Host),
				Key:    aws.String(bd.source.Path[1:]),
			})

			if err != nil {
				if aerr, ok := err.(awserr.RequestFailure); ok {
					switch aerr.StatusCode() {

					default:
						logger.Error(aerr.Error())
					}
				} else {
					// Print the error, cast err to awserr.Error to get the Code and
					// Message from an error.
					logger.Error(err.Error())
				}
				return
			}
		}
	}

}

// NewBucketDeleter creates a new BucketDeleter struct initialized with all variables needed to list a bucket
func NewBucketDeleter(source string, quite bool, threads int, versions bool, recursive bool, sess *session.Session) (*BucketDeleter, error) {

	sourceURL, err := url.Parse(source)
	if err != nil {
		return nil, err
	}

	if sourceURL.Scheme != "s3" {
		return nil, errors.New("usage: aws s3 ls <S3Uri> ")

	}

	bd := &BucketDeleter{
		source:    *sourceURL,
		quiet:     quite,
		wg:        sync.WaitGroup{},
		total:     0,
		recursive: recursive,
	}

	bd.lister, err = NewBucketLister(source, threads, sess)

	if err != nil {
		return nil, err
	}

	if versions {
		bd.versions = make(chan []*s3.ObjectIdentifier, threads)
		bd.lister.versions = bd.versions
	} else {
		bd.objects = make(chan []*s3.Object, threads)
		bd.lister.objects = bd.objects
	}

	bd.svc, err = checkBucket(sess, sourceURL.Host, nil)
	if err != nil {
		return nil, err
	}

	return bd, nil
}
