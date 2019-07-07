package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"runtime"
	"sync"

	"github.com/aws/aws-sdk-go/aws/awsutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"go.uber.org/zap"
)

const chunkSize int64 = 5 * 1024 * 1024

type chunk struct {
	buffer   io.ReadCloser
	start    int64
	finish   int64
	num      int64
	template s3.UploadPartInput
}

var chunkThreads = make(semaphore, 20)

type RemoteCopy struct {
	cp          *BucketCopier
	chunkThread semaphore
	wg          sync.WaitGroup
}

func (rp *RemoteCopy) downloadChunks(object *s3.Object, chunks chan chunk) error {
	var logger = zap.S()

	var first int64
	last := chunkSize

	downloadInput := s3.GetObjectInput{
		Bucket: aws.String(rp.cp.source.Host),
		Key:    object.Key,
	}

	parts := ((*object.Size) / chunkSize) + 1

	for num := int64(1); num < parts+1; num++ {
		if last >= *object.Size {
			last = *object.Size - 1
		}

		downloadInput.Range = aws.String(fmt.Sprintf("bytes=%d-%d", first, last))

		chunkThreads.acquire(1)

		resp, err := rp.cp.downloadManager.S3.GetObject(&downloadInput)

		if err != nil {
			if aerr, ok := err.(awserr.RequestFailure); ok {
				switch aerr.StatusCode() {

				default:
					logger.Error(*object.Key)
					logger.Error(aerr.Error())
				}
			} else {
				// Print the error, cast err to awserr.Error to get the Code and
				// Message from an error.
				logger.Error(err.Error())
			}

			return err

		}

		chunks <- chunk{
			buffer: resp.Body,
			start:  first,
			finish: last,
			num:    num,
		}

		first = last + 1
		last = first + chunkSize
	}

	close(chunks)

	return nil
}

func (rp *RemoteCopy) uploadChunk(key *string, uploadId *string, wg *sync.WaitGroup, parts int64) (func(chunk chunk), s3.CompletedMultipartUpload) {
	var logger = zap.S()

	var cmu s3.CompletedMultipartUpload
	cmu.Parts = make([]*s3.CompletedPart, parts)

	input := s3.UploadPartInput{
		Bucket:   aws.String(rp.cp.dest.Host),
		Key:      key,
		UploadId: uploadId,
	}

	return func(chunk chunk) {
		defer wg.Done()

		bufferLength := (chunk.finish - chunk.start) + 1
		buffer := make([]byte, bufferLength)

		buffer, err := ioutil.ReadAll(chunk.buffer)

		writer := bytes.NewReader(buffer)

		input.Body = writer
		input.PartNumber = aws.Int64(chunk.num)

		resp, err := rp.cp.uploadManager.S3.UploadPart(&input)

		chunkThreads.release(1)
		cmu.Parts[chunk.num-1] = &s3.CompletedPart{
			ETag:       resp.ETag,
			PartNumber: aws.Int64(chunk.num),
		}

		if err != nil {

			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				default:
					logger.Error(aerr.Error())
				}
			} else {
				// Message from an error.
				logger.Error(err.Error())
			}

		}

		if !rp.cp.quiet {
			rp.cp.bars.fileSize.IncrInt64(bufferLength)
		}

	}, cmu
}

//
func (rp *RemoteCopy) uploadChunks(bucket *string, key *string, uploadId *string, chunks chan chunk, parts int64) error {

	var wg sync.WaitGroup
	uploadChunkFunc, cmu := rp.uploadChunk(key, uploadId, &wg, parts)

	for chunk := range chunks {
		wg.Add(1)
		go uploadChunkFunc(chunk)
	}

	wg.Wait()
	_, err := rp.cp.uploadManager.S3.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		MultipartUpload: &cmu,
		Bucket:          bucket,
		Key:             key,
		UploadId:        uploadId,
	})

	if !rp.cp.quiet {
		rp.cp.bars.count.Increment()
	}

	return err

}

func (rp *RemoteCopy) copySingleOperationWithDestinationProfile(object *s3.Object) error {
	var logger = zap.S()

	downloadInput := s3.GetObjectInput{
		Bucket: aws.String(rp.cp.source.Host),
		Key:    object.Key,
	}

	buffer := make([]byte, *object.Size)
	writeBuffer := aws.NewWriteAtBuffer(buffer)

	_, err := rp.cp.downloadManager.Download(writeBuffer, &downloadInput)

	if err != nil {
		if aerr, ok := err.(awserr.RequestFailure); ok {
			switch aerr.StatusCode() {

			default:
				logger.Error(*object.Key)
				logger.Error(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			logger.Error(err.Error())
		}

		return err

	}

	// Upload the file to S3.
	input := rp.cp.template
	input.Key = aws.String(rp.cp.dest.Path + "/" + (*object.Key)[len(rp.cp.source.Path):])
	input.Body = bytes.NewReader(writeBuffer.Bytes())
	_, err = rp.cp.uploadManager.Upload(&input)

	if err != nil {

		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				logger.Error(aerr.Error())
			}
		} else {
			// Message from an error.
			logger.Error(err.Error())
		}

	}

	if !rp.cp.quiet {
		rp.cp.bars.count.Increment()
		rp.cp.bars.fileSize.IncrInt64(*object.Size)
	}
	return nil
}

func (rp *RemoteCopy) remoteCopyObject() (func(object *s3.Object) error, error) {

	return func(object *s3.Object) error {
		defer rp.cp.threads.release(1)

		if *object.Size <= chunkSize {
			chunkThreads.acquire(1)
			err := rp.copySingleOperationWithDestinationProfile(object)
			chunkThreads.release(1)
			return err
		}

		//Create the multipart upload
		params := &s3.CreateMultipartUploadInput{}
		awsutil.Copy(params, object)
		params.Bucket = aws.String(rp.cp.dest.Host)
		params.Key = aws.String(rp.cp.dest.Path + "/" + (*object.Key)[len(rp.cp.source.Path):])

		// Create the multipart
		resp, err := rp.cp.uploadManager.S3.CreateMultipartUpload(params)
		if err != nil {
			return err
		}

		parts := ((*object.Size) / chunkSize) + 1

		chunks := make(chan chunk, 20)

		go rp.downloadChunks(object, chunks)

		err = rp.uploadChunks(params.Bucket, aws.String(rp.cp.dest.Path+"/"+(*object.Key)[len(rp.cp.source.Path):]), resp.UploadId, chunks, parts)

		return err
	}, nil
}

func (rp *RemoteCopy) remoteCopy() error {
	defer rp.cp.wg.Done()

	copyObjectsFunc, err := rp.remoteCopyObject()

	if err != nil {
		return err
	}
	allThreads := cap(rp.cp.threads)
	if !rp.cp.quiet {
		fmt.Printf("0")
	}

	//we need one thread to update the progress bar and another to do the downloads

	for item := range rp.cp.srcObjects {

		for _, object := range item {
			rp.cp.threads.acquire(1)
			go copyObjectsFunc(object)
			runtime.GC()
		}

	}
	rp.cp.threads.acquire(allThreads)

	return nil
}

func newRemoteCopier(cp *BucketCopier) *RemoteCopy {
	return &RemoteCopy{
		cp:          cp,
		chunkThread: make(semaphore, 20),
	}

}
