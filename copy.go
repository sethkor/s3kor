package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/vbauerster/mpb/decor"

	"github.com/vbauerster/mpb"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"go.uber.org/zap"
)

//The different type of progress bars.  We have one for counting files and another for counting sizes
type copyPb struct {
	count    *mpb.Bar
	fileSize *mpb.Bar
}

type BucketCopier struct {
	source        url.URL
	target        url.URL
	sourceLength  int
	uploadManager s3manager.Uploader
	bars          copyPb
	wg            sync.WaitGroup
	files         chan fileJob
	fileCounter   chan int64
	threads       semaphore
	template      s3manager.UploadInput
}

func (copier *BucketCopier) copyFile(file string) {
	var logger = zap.S()

	f, err := os.Open(file)
	if err != nil {
		logger.Errorf("failed to open file %q, %v", file, err)
	} else {
		// Upload the file to S3.
		input := copier.template
		input.Key = aws.String(copier.target.Path + "/" + file[copier.sourceLength:])
		input.Body = f
		_, err = copier.uploadManager.Upload(&input)

		if err != nil {
			logger.Error("Object failed to create in S3 ", file)

			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				default:
					logger.Error(aerr.Error())
				} //switch
			} else {
				// Message from an error.
				logger.Error(err.Error())
			} //else
		}
		_ = f.Close()
		logger.Debug("file>>>s3 ", file)
	} //else
}

func (copier *BucketCopier) uploadFile() func(file fileJob) {
	var logger = zap.S()

	return func(file fileJob) {
		defer copier.threads.release(1)
		start := time.Now()
		input := copier.template
		if file.info.IsDir() {
			//Don't create a prefix for the base dir
			if len(file.path) != copier.sourceLength {
				input.Key = aws.String(copier.target.Path + "/" + file.path[copier.sourceLength:] + "/")
				_, err := copier.uploadManager.Upload(&input)

				if err != nil {
					logger.Error("Prefix failed to create in S3 ", file.path)

					if aerr, ok := err.(awserr.Error); ok {
						switch aerr.Code() {
						default:
							logger.Error(aerr.Error())
						} //switch
					} else {
						// Message from an error.
						logger.Error(err.Error())
					} //else
					return
				}
				logger.Info("dir>>>>s3 ", file.path)
			} //if
		} else {
			copier.copyFile(file.path)
		} //else
		copier.bars.count.IncrInt64(1)
		copier.bars.fileSize.IncrInt64(file.info.Size(), time.Since(start))
	}
}

func (copier *BucketCopier) processFiles() {
	defer copier.wg.Done()

	allThreads := len(copier.threads)
	uploadFileFunc := copier.uploadFile()
	for file := range copier.files {
		copier.threads.acquire(1) // or block until one slot is free
		go uploadFileFunc(file)
	} //for
	copier.threads.acquire(allThreads) // don't continue until all goroutines complete

}

func (pb copyPb) updateBar(fileSize <-chan int64, wg *sync.WaitGroup) {
	defer wg.Done()
	var fileCount int64 = 0
	var fileSizeTotal int64 = 0

	//var chunk int64 = 0
	for size := range fileSize {
		fileCount++
		fileSizeTotal += size
		pb.count.SetTotal(fileCount, false)
		pb.fileSize.SetTotal(fileSizeTotal, false)
	}

}

func (copier *BucketCopier) copy(recursive bool) {
	//var logger = zap.S()

	if recursive {
		if copier.source.Scheme != "s3" {

			go walkFiles(copier.source.Path, copier.files, copier.fileCounter)
		}

		progress := mpb.New(
			mpb.WithRefreshRate(1000 * time.Millisecond),
		)

		copier.bars.count = progress.AddBar(0,
			mpb.PrependDecorators(
				// simple name decorator
				decor.Name("Files", decor.WC{W: 6, C: decor.DSyncWidth}),
				decor.CountersNoUnit(" %d / %d", decor.WCSyncWidth),
			),
		)

		copier.bars.fileSize = progress.AddBar(0,
			mpb.PrependDecorators(
				decor.Name("Size ", decor.WC{W: 6, C: decor.DSyncWidth}),
				decor.Counters(decor.UnitKB, "% .1f / % .1f", decor.WCSyncWidth),
			),
			mpb.AppendDecorators(
				decor.Percentage(decor.WCSyncWidth),
				decor.Name(" "),
				decor.AverageSpeed(decor.UnitKB, "% .1f", decor.WCSyncWidth),
				decor.Name(" ETA: "),
				decor.AverageETA(decor.ET_STYLE_GO),
			),
		)

		copier.wg.Add(2)

		go copier.bars.updateBar(copier.fileCounter, &copier.wg)

		go copier.processFiles()

		copier.wg.Wait()

		progress.Wait()
	} else {
		//single file copy
		info, err := os.Lstat(copier.source.Path)

		if err != nil {
			if info.IsDir() {
				fmt.Println("Can not coppy a directory without --recursive specified on command line")
			} else {
				copier.copyFile(copier.source.Path)
			}
		}
	}

}

func NewBucketCopier(source string, dest string, threads int, sess *session.Session, template s3manager.UploadInput) (*BucketCopier, error) {

	var svc *s3.S3 = nil
	sourceURL, err := url.Parse(source)
	if err != nil {
		return nil, err
	}

	destURL, err := url.Parse(dest)
	if err != nil {
		return nil, err
	}

	if sourceURL.Scheme != "s3" && destURL.Scheme != "s3" {
		return nil, errors.New("usage: aws s3 cp <LocalPath> <S3Uri> or <S3Uri> <LocalPath> or <S3Uri> <S3Uri>")

	}

	if sourceURL.Scheme == "s3" {
		svc, err = checkBucket(sess, sourceURL.Host)
		if err != nil {
			return nil, err
		}
	}

	if destURL.Scheme == "s3" {
		svc, err = checkBucket(sess, destURL.Host)
		if err != nil {
			return nil, err
		}
	}

	template.Bucket = aws.String(destURL.Host)

	bc := &BucketCopier{
		source:        *sourceURL,
		target:        *destURL,
		uploadManager: *s3manager.NewUploaderWithClient(svc),
		threads:       make(semaphore, threads),
		files:         make(chan fileJob, bigChanSize),
		fileCounter:   make(chan int64, threads*2),
		wg:            sync.WaitGroup{},
		template:      template,
	}

	//Some logic to determin the base path to be used as the prefix for S3.  If the source pass ends with a "/" then
	//the base of the source path is not used in the S3 prefix as we assume iths the contents of the directory, not
	//the actual directory that is needed in the copy
	_, splitFile := filepath.Split(bc.source.Path)
	includeRoot := 0
	if splitFile != "" {
		includeRoot = len(splitFile)
	}

	bc.sourceLength = len(bc.source.Path) - includeRoot
	if len(bc.source.Path) == 0 {
		bc.sourceLength++

	}
	return bc, nil
}
