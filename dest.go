package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"sync"

	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/vbauerster/mpb/v5"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

const chunkSize int64 = 5 * 1024 * 1024

type chunk struct {
	buffer io.ReadCloser
	start  int64
	finish int64
	num    int64
}

type remoteCopy struct {
	*BucketCopier
	chunkThreads semaphore
	progress     *mpb.Progress
}

func (rp *remoteCopy) downloadChunks(object *s3.Object, chunks chan chunk) {

	var first int64
	last := chunkSize

	downloadInput := s3.GetObjectInput{
		Bucket: aws.String(rp.source.Host),
		Key:    object.Key,
	}

	parts := ((*object.Size) / chunkSize) + 1

	for num := int64(1); num < parts+1; num++ {
		if last >= *object.Size {
			last = *object.Size - 1
		}

		downloadInput.Range = aws.String(fmt.Sprintf("bytes=%d-%d", first, last))

		rp.chunkThreads.acquire(1)

		resp, err := rp.downloadManager.S3.GetObject(&downloadInput)

		if err != nil {
			rp.errors <- copyError{error: err}
			return
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

}

func (rp *remoteCopy) uploadChunk(input *s3.UploadPartInput, chunk chunk, cmu *s3.CompletedMultipartUpload, wg *sync.WaitGroup) {

	defer wg.Done()
	defer rp.chunkThreads.release(1)

	buffer, err := ioutil.ReadAll(chunk.buffer)
	if err != nil {
		rp.errors <- copyError{error: err}
		if !rp.quiet && rp.bars.fileSize != nil {
			rp.bars.fileSize.IncrInt64((chunk.finish - chunk.start) + 1)
		}
		return
	}

	writer := bytes.NewReader(buffer)

	input.Body = writer
	input.PartNumber = aws.Int64(chunk.num)

	resp, err := rp.uploadManager.S3.UploadPart(input)

	if !rp.quiet && rp.bars.fileSize != nil {
		rp.bars.fileSize.IncrInt64((chunk.finish - chunk.start) + 1)
	}

	if err != nil {
		rp.errors <- copyError{error: err}
		return
	}

	cmu.Parts[chunk.num-1] = &s3.CompletedPart{
		ETag:       resp.ETag,
		PartNumber: aws.Int64(chunk.num),
	}

}

func (rp *remoteCopy) uploadChunks(bucket *string, key *string, uploadID *string, chunks chan chunk, parts int64) {

	var wg sync.WaitGroup

	var cmu s3.CompletedMultipartUpload
	cmu.Parts = make([]*s3.CompletedPart, parts)

	input := s3.UploadPartInput{
		Bucket:   aws.String(rp.dest.Host),
		Key:      key,
		UploadId: uploadID,
	}

	for chunk := range chunks {
		wg.Add(1)
		go rp.uploadChunk(&input, chunk, &cmu, &wg)
	}

	wg.Wait()

	if !rp.quiet {
		rp.bars.count.Increment()
	}

	// check to see if all parts are here.   If they are not there must have been an error so abort the multipart upload
	ok := true
	for i := int64(0); i < parts; i++ {
		if cmu.Parts[i] == nil {
			ok = false
			break
		}
	}

	var err error

	if ok {
		_, err = rp.uploadManager.S3.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
			MultipartUpload: &cmu,
			Bucket:          bucket,
			Key:             key,
			UploadId:        uploadID,
		})

	} else {
		_, err = rp.uploadManager.S3.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
			Bucket:   bucket,
			Key:      key,
			UploadId: uploadID,
		})
	}

	if err != nil {
		rp.errors <- copyError{error: err}
	}

}

func (rp *remoteCopy) copySingleOperationWithDestinationProfile(object *s3.Object) {

	downloadInput := s3.GetObjectInput{
		Bucket: aws.String(rp.source.Host),
		Key:    object.Key,
	}

	buffer := make([]byte, *object.Size)
	writeBuffer := aws.NewWriteAtBuffer(buffer)

	_, err := rp.downloadManager.Download(writeBuffer, &downloadInput)

	if err != nil {
		rp.errors <- copyError{error: err}
		return
	}

	// Upload the file to S3.
	input := rp.template
	input.Key = aws.String(rp.dest.Path + "/" + (*object.Key)[len(rp.source.Path):])
	input.Body = bytes.NewReader(writeBuffer.Bytes())
	_, err = rp.uploadManager.Upload(&input)

	if !rp.quiet {
		rp.bars.count.Increment()
		if rp.bars.fileSize != nil {
			rp.bars.fileSize.IncrInt64(*object.Size)
		}
	}

	if err != nil {
		rp.errors <- copyError{error: err}
		return
	}

}

func (rp *remoteCopy) remoteCopyObject(object *s3.Object) {

	defer rp.threads.release(1)

	if *object.Size <= chunkSize {
		rp.chunkThreads.acquire(1)
		rp.copySingleOperationWithDestinationProfile(object)
		rp.chunkThreads.release(1)

		return
	}

	//Create the multipart upload
	params := &s3.CreateMultipartUploadInput{}
	awsutil.Copy(params, object)
	params.Bucket = aws.String(rp.dest.Host)
	params.Key = aws.String(rp.dest.Path + "/" + (*object.Key)[len(rp.source.Path):])

	// Create the multipart
	resp, err := rp.uploadManager.S3.CreateMultipartUpload(params)
	if err != nil {
		rp.errors <- copyError{error: err}
		return
	}

	parts := ((*object.Size) / chunkSize) + 1

	chunks := make(chan chunk, 20)

	go rp.downloadChunks(object, chunks)

	rp.uploadChunks(params.Bucket, aws.String(rp.dest.Path+"/"+(*object.Key)[len(rp.source.Path):]), resp.UploadId, chunks, parts)

}

func (rp *remoteCopy) remoteCopy() {
	defer rp.wg.Done()
	allThreads := cap(rp.threads)

	//we need one thread to update the progress bar and another to do the downloads

	for item := range rp.srcObjects {

		for _, object := range item {
			rp.threads.acquire(1)
			go rp.remoteCopyObject(object)
		}

	}
	rp.threads.acquire(allThreads)
	close(rp.errors)

}

func newRemoteCopier(cp *BucketCopier, progress *mpb.Progress) *remoteCopy {
	return &remoteCopy{
		BucketCopier: cp,
		chunkThreads: make(semaphore, 5),
		progress:     progress,
	}

}
