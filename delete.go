package main

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type deleteError struct {
	error             error
	deleteObjectInput *s3.DeleteObjectsInput
}

type deleteErrorList struct {
	errorList []deleteError
}

// BucketDeleter stores everything we need to delete srcObjects in a bucket
type BucketDeleter struct {
	source    url.URL
	quiet     bool
	recursive bool
	multiPart bool
	total     int64
	objects   chan []*s3.Object
	versions  chan []*s3.ObjectIdentifier
	wg        sync.WaitGroup
	ewg       sync.WaitGroup
	svc       *s3.S3
	lister    *BucketLister
	errors    chan deleteError
	errorList deleteErrorList
}

func (de deleteError) Error() string {
	return de.error.Error()
}

func (del deleteErrorList) Error() string {
	if len(del.errorList) > 0 {
		out := make([]string, len(del.errorList))
		for i, err := range del.errorList {
			out[i] = err.Error()
		}
		return strings.Join(out, "\n")
	}
	return ""
}

// collectErrors processes any any errors passed via the error channel
// and stores them in the errorList
func (bd *BucketDeleter) collectErrors() {
	defer bd.ewg.Done()
	for err := range bd.errors {
		bd.errorList.errorList = append(bd.errorList.errorList, err)
	}
}

func (bd *BucketDeleter) deleteObjects() func(item []*s3.ObjectIdentifier) {
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
			fmt.Printf("\r%d Deleted", bd.total)
		}

		if err != nil {
			bd.errors <- deleteError{
				error:             err,
				deleteObjectInput: &deleteInput,
			}
		}

	}
}

func (bd *BucketDeleter) deleteAllObjects(versions bool) {
	deleteObjectsFunc := bd.deleteObjects()
	if !bd.quiet {
		fmt.Printf("0 Deleted")
	}

	bd.ewg.Add(1) // a separate error waitgroup so we wait until all errors are reported before exiting

	if versions {
		for item := range bd.versions {
			bd.wg.Add(1)
			go deleteObjectsFunc(item)
		}
	} else {
		for item := range bd.objects {

			//convert from srcObjects to object identifiers
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
	close(bd.errors)

}

func (bd *BucketDeleter) abortMultiPartUploads() {
	defer bd.wg.Done()
	err := bd.svc.ListMultipartUploadsPages(&s3.ListMultipartUploadsInput{
		Bucket: aws.String(bd.source.Host),
	}, func(result *s3.ListMultipartUploadsOutput, lastPage bool) bool {

		for _, upload := range result.Uploads {
			_, err := bd.svc.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
				Bucket:   result.Bucket,
				Key:      upload.Key,
				UploadId: upload.UploadId,
			})

			if err != nil {
				bd.errors <- deleteError{
					error: err,
				}
			}
		}
		return true
	})
	if err != nil {
		bd.errors <- deleteError{
			error: err,
		}
	}
}

func (bd *BucketDeleter) delete() error {

	var err error

	versions := bd.versions != nil

	if versions || bd.recursive {

		if bd.multiPart {
			bd.wg.Add(1)
			go bd.abortMultiPartUploads()
		}

		go bd.collectErrors()
		go bd.deleteAllObjects(versions)

		if versions {
			// bd.recursive determines if an exact match is looked for when looking for versions.  if the value is true
			// then an exact match is not searched for.
			err = bd.lister.listObjectVersions(!bd.recursive)
		} else {

			err = bd.lister.listObjects(false)
		}

		if err != nil {
			return err
		}
		//Wait for any errors to be reported before exiting
		bd.wg.Wait()
		bd.ewg.Wait()

		if len(bd.errorList.errorList) > 0 {
			return bd.errorList
		}
	} else {

		_, err = bd.svc.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(bd.source.Host),
			Key:    aws.String(bd.source.Path[1:]),
		})

	}
	return err

}

// NewBucketDeleter creates a new BucketDeleter struct initialized with all variables needed to list a bucket
func NewBucketDeleter(source string, quite bool, threads int, versions bool, recursive bool, multiPart bool, sess *session.Session) (*BucketDeleter, error) {

	sourceURL, err := url.Parse(source)
	if err != nil {
		return nil, err
	}

	if sourceURL.Scheme != "s3" {
		return nil, errors.New("usage: aws s3 ls <S3Uri> ")

	}

	if !recursive || !versions {
		if sourceURL.Path == "" {
			return nil, errors.New("Must pass an object in the bucket to remove, not just the bucket name or use --recursive instead")
		}
	}

	bd := &BucketDeleter{
		source:    *sourceURL,
		quiet:     quite,
		wg:        sync.WaitGroup{},
		total:     0,
		recursive: recursive,
		multiPart: multiPart,
		errors:    make(chan deleteError, threads),
	}

	bd.lister, err = NewBucketLister(source, versions, threads, sess)

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
