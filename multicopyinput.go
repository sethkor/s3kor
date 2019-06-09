package main

import "github.com/aws/aws-sdk-go/service/s3"

// MultiCopyInput provides the input parameters for Copying a stream or buffer
// to an object in an Amazon S3 bucket. This type is similar to the s3
// package's PutObjectInput with the exception that the Body member is an
// io.Reader instead of an io.ReadSeeker.
type MultiCopyInput struct {
	Input *s3.CopyObjectInput

	Bucket *string
}
