package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"go.uber.org/zap"
)

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