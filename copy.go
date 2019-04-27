package main

import (
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/vbauerster/mpb/decor"

	"github.com/vbauerster/mpb"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"go.uber.org/zap"
)

func uploadFile(targetBucket string, sourceLength int, uploader s3manager.Uploader, bar *mpb.Bar, barSize *mpb.Bar, semStreams semaphore) func(file fileJob) {
	var logger = zap.S()
	return func(file fileJob) {
		defer semStreams.release(1)
		start := time.Now()
		if file.info.IsDir() {
			//Don't create a prefix for the base dir
			if len(file.path) != sourceLength {
				_, err := uploader.Upload(&s3manager.UploadInput{
					Bucket: aws.String(targetBucket),
					ACL:    aws.String(s3.ObjectCannedACLBucketOwnerFullControl),
					Key:    aws.String(file.path[sourceLength:] + "/"),
				})

				if err != nil {
					logger.Error("Prefix failed to create in S3 ", file.path)

					if aerr, ok := err.(awserr.Error); ok {
						switch aerr.Code() {
						default:
							logger.Error(aerr.Error())
						} //switch
					} else {
						// Print the error, cast err to awserr.Error to get the Code and
						// Message from an error.
						logger.Error(err.Error())
					} //else
					return
				}
				logger.Info("dir>>>>s3 ", file.path)
			} //if
		} else {

			f, err := os.Open(file.path)
			if err != nil {
				logger.Errorf("failed to open file %q, %v", file.path, err)
			} else {
				// Upload the file to S3.

				_, err = uploader.Upload(&s3manager.UploadInput{
					Bucket: aws.String(targetBucket),
					ACL:    aws.String(s3.ObjectCannedACLBucketOwnerFullControl),
					Key:    aws.String(file.path[sourceLength:]),
					Body:   f,
				})

				if err != nil {
					logger.Error("Object failed to create in S3 ", file.path)

					if aerr, ok := err.(awserr.Error); ok {
						switch aerr.Code() {
						default:
							logger.Error(aerr.Error())
						} //switch
					} else {
						// Print the error, cast err to awserr.Error to get the Code and
						// Message from an error.
						logger.Error(err.Error())
					} //else
					return
				}
				_ = f.Close()
				logger.Debug("file>>>s3 ", file.path)
			} //else
		} //else
		//bar.Increment()
		bar.IncrBy(1, time.Since(start))

		//Some logic to chunk up int64 as the progress bar library will only let me pass a 32 bit value for now
		if file.info.Size() > math.MaxInt32 {
			fileSize := file.info.Size()

			for {
				barSize.IncrBy(math.MaxInt32, time.Duration(0))
				fileSize -= math.MaxInt32
				if fileSize < math.MaxUint32 {
					break
				}
			}
			barSize.IncrBy(int(fileSize), time.Since(start))

		} else {
			barSize.IncrBy(int(file.info.Size()), time.Since(start))
		}

	}
}

func processFiles(sourceDir url.URL, targetBucket url.URL, includeRoot int, files <-chan fileJob, threads int, bar *mpb.Bar, barBytes *mpb.Bar, sess *session.Session, wg *sync.WaitGroup) {
	defer wg.Done()

	semStreams := make(semaphore, threads)

	uploader := s3manager.NewUploader(sess)

	sourceLength := len(sourceDir.Path) - includeRoot

	if len(sourceDir.Path) == 0 {
		sourceLength++

	}

	uploadFileFunc := uploadFile(targetBucket.Host, sourceLength, *uploader, bar, barBytes, semStreams)
	for file := range files {
		semStreams.acquire(1) // or block until one slot is free
		go uploadFileFunc(file)
	} //for
	semStreams.acquire(threads) // don't continue until all goroutines complete

}

func updateBar(bar *mpb.Bar, barBytes *mpb.Bar, fileSize <-chan int64, wg *sync.WaitGroup) {
	defer wg.Done()
	var fileCount int64 = 0
	var fileSizeTotal int64 = 0

	//var chunk int64 = 0
	for size := range fileSize {
		fileCount++
		fileSizeTotal += size
		bar.SetTotal(fileCount, false)
		barBytes.SetTotal(fileSizeTotal, false)
	}

}

func copy(sess *session.Session) {
	var logger = zap.S()

	sourceURL, err := url.Parse(*cpSource)
	destUrl, err := url.Parse(*cpDestination)

	var files chan fileJob
	fileCountSem := make(chan int64, 10000)

	if sourceURL.Scheme != "s3" {
		files = make(chan fileJob, 10000)
		go walkFiles(sourceURL.Path, files, fileCountSem)
	}
	//
	//if destUrl.Scheme == "s3" {
	//	_, err = checkBucket(sess, destUrl.Host, autoRegion)
	//}
	//get all the aws bits ready

	if err == nil {

		progress := mpb.New()

		bar := progress.AddBar(0,
			mpb.PrependDecorators(
				// simple name decorator
				decor.Name("Files", decor.WC{W: 6, C: decor.DSyncWidth}),
				decor.CountersNoUnit(" %d / %d", decor.WCSyncWidth),
			),

			mpb.AppendDecorators(
				decor.Percentage(decor.WCSyncWidth),
				decor.Name(" "),
				decor.MovingAverageETA(decor.ET_STYLE_GO, decor.NewMedian(), decor.FixedIntervalTimeNormalizer(5000), decor.WCSyncSpaceR),
			),
		)

		barBytes := progress.AddBar(0,
			mpb.PrependDecorators(
				decor.Name("Size ", decor.WC{W: 6, C: decor.DSyncWidth}),
				decor.Counters(decor.UnitKB, "% .1f / % .1f", decor.WCSyncWidth),
			),
			mpb.AppendDecorators(
				decor.Percentage(decor.WCSyncWidth),
				decor.Name(" "),
				decor.AverageSpeed(decor.UnitKB, "% .1f", decor.WCSyncWidth),
			),
		)

		var wg sync.WaitGroup
		wg.Add(2)

		_, splitFile := filepath.Split(sourceURL.Path)
		includeRoot := 0
		if splitFile != "" {
			includeRoot = len(splitFile)
		}

		go updateBar(bar, barBytes, fileCountSem, &wg)

		go processFiles(*sourceURL, *destUrl, includeRoot, files, 100, bar, barBytes, sess, &wg)

		wg.Wait()

		progress.Wait()

	} else {
		fmt.Println("S3 URL passed not formatted correctly")
		logger.Fatal("S3 URL passed not formatted correctly")
	}
}
