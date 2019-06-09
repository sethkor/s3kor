package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"go.uber.org/zap"
)

// checkBucket checks a buckets region.  We use the HeadObject function as this is can be used anonymously and is not
// subject to the buckets policy like GetBucketRegion.  The region is retorned in the header of the HTTP response.
func checkBucket(sess *session.Session, bucket string) (svc *s3.S3, err error) {

	var logger = zap.S()

	svc = s3.New(session.Must(session.NewSession(&aws.Config{
		Credentials: credentials.AnonymousCredentials,
		Region:      sess.Config.Region,
	})))

	req, _ := svc.HeadBucketRequest(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})

	err = req.Send()

	if err != nil {
		fmt.Println(err)

		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {

			case s3.ErrCodeNoSuchBucket:
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

	req.HTTPResponse.Header.Get("X-Amz-Bucket-Region")
	svc = s3.New(sess, &aws.Config{MaxRetries: aws.Int(30),
		Region: aws.String(s3.NormalizeBucketLocation(req.HTTPResponse.Header.Get("X-Amz-Bucket-Region")))})

	return svc, err
}
