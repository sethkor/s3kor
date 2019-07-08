package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/vbauerster/mpb/decor"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/vbauerster/mpb"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"go.uber.org/zap"
)

// The different type of progress bars.  We have one for counting files and another for counting sizes
type copyPb struct {
	count    *mpb.Bar
	fileSize *mpb.Bar
}

// BucketCopier stores everything we need to copy srcObjects to or from a bucket
type BucketCopier struct {
	source          url.URL
	dest            url.URL
	recursive       bool
	quiet           bool
	sourceLength    int
	uploadManager   s3manager.Uploader
	downloadManager s3manager.Downloader
	svc             *s3.S3
	destSvc         *s3.S3
	bars            copyPb
	wg              sync.WaitGroup
	files           chan fileJob
	fileCounter     chan int64
	versions        chan []*s3.ObjectIdentifier
	srcObjects      chan []*s3.Object
	sizeChan        chan objectCounter
	threads         semaphore
	template        s3manager.UploadInput
	srcLister       *BucketLister
}

func (cp *BucketCopier) updateBars(count int64, size int64, timeSince time.Duration) {
	if !cp.quiet {
		cp.bars.count.IncrInt64(count)
		if cp.bars.fileSize != nil {
			cp.bars.fileSize.IncrInt64(size, timeSince)
		}
	}
}

func (cp *BucketCopier) copyFile(file string) {
	var logger = zap.S()

	f, err := os.Open(filepath.Clean(file))
	if err != nil {
		logger.Errorf("failed to open file %q, %v", file, err)
	} else {
		// Upload the file to S3.
		input := cp.template
		input.Key = aws.String(cp.dest.Path + "/" + file[cp.sourceLength:])
		input.Body = f
		_, err = cp.uploadManager.Upload(&input)

		if err != nil {
			logger.Error("Object failed to create in S3 ", file)

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
		err = f.Close()
		if err != nil {
			logger.Error(err)
		}
		logger.Debug("file>>>s3 ", file)
	}
}

func (cp *BucketCopier) uploadFile() func(file fileJob) {
	var logger = zap.S()

	return func(file fileJob) {
		defer cp.threads.release(1)
		start := time.Now()
		input := cp.template
		if file.info.IsDir() {
			// Don't create a prefix for the base dir
			if len(file.path) != cp.sourceLength {
				input.Key = aws.String(cp.dest.Path + "/" + file.path[cp.sourceLength:] + "/")
				_, err := cp.uploadManager.Upload(&input)

				if err != nil {
					logger.Error("Prefix failed to create in S3 ", file.path)

					if aerr, ok := err.(awserr.Error); ok {
						switch aerr.Code() {
						default:
							logger.Error(aerr.Error())
						}
					} else {
						// Message from an error.
						logger.Error(err.Error())
					}
					return
				}
				logger.Info("dir>>>>s3 ", file.path)
			}
		} else {
			cp.copyFile(file.path)
		}
		cp.updateBars(1, file.info.Size(), time.Since(start))
	}
}

func (cp *BucketCopier) processFiles() {

	allThreads := cap(cp.threads)
	uploadFileFunc := cp.uploadFile()
	for file := range cp.files {
		cp.threads.acquire(1) // or block until one slot is free
		go uploadFileFunc(file)
	}
	cp.threads.acquire(allThreads) // don't continue until all goroutines complete

}

func (pb copyPb) updateBar(fileSize <-chan int64, wg *sync.WaitGroup) {
	defer wg.Done()
	var fileCount int64
	var fileSizeTotal int64

	for size := range fileSize {
		fileCount++
		fileSizeTotal += size
		pb.count.SetTotal(fileCount, false)
		if pb.fileSize != nil {
			pb.fileSize.SetTotal(fileSizeTotal, false)
		}
	}

}

func (pb copyPb) updateBarListObjects(fileSize <-chan objectCounter, wg *sync.WaitGroup) {
	defer wg.Done()
	var fileCount int64
	var fileSizeTotal int64

	for size := range fileSize {
		fileCount += int64(size.count)
		fileSizeTotal += size.size
		pb.count.SetTotal(fileCount, false)
		if pb.fileSize != nil {
			pb.fileSize.SetTotal(fileSizeTotal, false)
		}
	}
}

func (cp *BucketCopier) downloadObjects() (func(object *s3.Object) error, error) {
	var logger = zap.S()

	var dirs sync.Map
	theSeparator := string(os.PathSeparator)

	return func(object *s3.Object) error {
		defer cp.threads.release(1)
		start := time.Now()
		// Check File path and dir
		theFilePath := cp.dest.Path + theSeparator + *object.Key

		theDir := filepath.Dir(theFilePath)

		_, ok := dirs.Load(theDir)
		if !ok {

			_, err := os.Lstat(theDir)

			if os.IsNotExist(err) {
				err = os.MkdirAll(theDir, os.ModePerm)
			}
			dirs.Store(theDir, true)

			if err != nil {
				return err
			}
			for {
				theDir = filepath.Dir(theDir)
				if theDir == "/" || theDir == "." {
					break
				} else {
					dirs.Store(theDir, true)
				}
			}
		}

		theFile, err := os.Create(theFilePath)
		if err != nil {
			return err
		}

		downloadInput := s3.GetObjectInput{
			Bucket: aws.String(cp.source.Host),
			Key:    object.Key,
		}

		objectSize, err := cp.downloadManager.Download(theFile, &downloadInput)

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

		}

		cp.updateBars(1, objectSize, time.Since(start))

		err = theFile.Close()
		if err != nil {
			logger.Error(err)
		}
		return err
	}, nil
}

func (cp *BucketCopier) downloadAllObjects() error {
	defer cp.wg.Done()
	downloadObjectsFunc, err := cp.downloadObjects()

	if err != nil {
		return err
	}
	allThreads := cap(cp.threads)
	if !cp.quiet {
		fmt.Printf("0")
	}

	//we need one thread to update the progress bar and another to do the downloads

	for item := range cp.srcObjects {

		for _, object := range item {
			cp.threads.acquire(1)
			go downloadObjectsFunc(object)
		}

	}
	cp.threads.acquire(allThreads)

	return nil
}

func (cp *BucketCopier) copyObjects() (func(object *s3.Object) error, error) {
	var logger = zap.S()

	copyTemplate := s3.CopyObjectInput{
		ACL:                  cp.template.ACL,
		Bucket:               aws.String(cp.dest.Host),
		Key:                  aws.String(cp.dest.Path),
		ServerSideEncryption: cp.template.ServerSideEncryption,
	}

	if *cp.template.ServerSideEncryption != "" {
		copyTemplate.ServerSideEncryption = cp.template.ServerSideEncryption
	}
	cm := NewCopyerWithClient(cp.svc)

	return func(object *s3.Object) error {
		defer cp.threads.release(1)
		start := time.Now()
		copyInput := copyTemplate

		copyInput.CopySource = aws.String(cp.source.Host + "/" + *object.Key)

		if len(cp.source.Path) == 0 {
			copyInput.Key = object.Key
		} else if cp.source.Path[len(cp.source.Path)-1:] == "/" {
			copyInput.Key = aws.String(cp.dest.Path + "/" + (*object.Key)[len(cp.source.Path)-1:])
		} else {
			copyInput.Key = aws.String(cp.dest.Path + "/" + (*object.Key))
		}

		if *object.Size <= MaxCopyPartSize {

			_, err := cp.uploadManager.S3.CopyObject(&copyInput)

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
		} else {

			cmi := MultiCopyInput{
				Input:  &copyInput,
				Bucket: aws.String(cp.source.Host),
			}

			_, err := cm.Copy(&cmi)

			if err != nil {
				return err
			}
		}

		cp.updateBars(1, *object.Size, time.Since(start))

		return nil
	}, nil
}

func (cp *BucketCopier) copyAllObjects() error {
	defer cp.wg.Done()

	copyObjectsFunc, err := cp.copyObjects()

	if err != nil {
		return err
	}
	allThreads := cap(cp.threads)
	if !cp.quiet {
		fmt.Printf("0")
	}

	//we need one thread to update the progress bar and another to do the downloads

	for item := range cp.srcObjects {

		for _, object := range item {
			cp.threads.acquire(1)
			go copyObjectsFunc(object)
		}

	}
	cp.threads.acquire(allThreads)

	return nil
}

func (cp *BucketCopier) setupBars() *mpb.Progress {
	progress := mpb.New(mpb.WithWaitGroup(&cp.wg))

	cp.bars.count = progress.AddBar(0,
		mpb.PrependDecorators(
			// simple name decorator
			decor.Name("Files", decor.WC{W: 6, C: decor.DSyncWidth}),
			decor.CountersNoUnit(" %d / %d", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(
			// replace empty decorator with "done" message, OnComplete event
			decor.OnComplete(
				// Empty decorator
				decor.Name(""), "Done!",
			),
		),
	)

	cp.bars.fileSize = progress.AddBar(0,
		mpb.PrependDecorators(
			decor.Name("Size ", decor.WC{W: 6, C: decor.DSyncWidth}),
			decor.Counters(decor.UnitKB, "% .1f / % .1f", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(
			decor.Percentage(decor.WCSyncWidth),
			decor.Name(" "),
			decor.AverageSpeed(decor.UnitKB, "% .1f", decor.WCSyncWidth),
			decor.OnComplete(
				// Empty decorator
				decor.Name(""), " Done!",
			),
		),
	)

	return progress
}

func (cp *BucketCopier) checkPath(path string) (isDir bool, err error) {
	absPath, err := filepath.Abs(path)
	if err == nil {
		info, err := os.Lstat(absPath)
		if err == nil {
			return info.IsDir(), err
		}
	}
	errString := "The user-provided path" + cp.dest.Path + "does not exsit"
	fmt.Println(errString)
	return false, errors.New(errString)
}

func (cp *BucketCopier) copyS3ToS3() error {
	if !cp.quiet {
		go cp.bars.updateBarListObjects(cp.srcLister.sizeChan, &cp.wg)
	}
	// List Objects
	go cp.srcLister.ListObjects(true)

	var err error
	if cp.destSvc == nil {
		err = cp.copyAllObjects()
	} else {
		rp := newRemoteCopier(cp)
		err = rp.remoteCopy()
		cp.wg.Done()
	}
	return err
}

func (cp *BucketCopier) copyFileToS3() error {
	isDir, err := cp.checkPath(cp.source.Path)

	if err == nil {

		if cp.recursive {

			if !cp.quiet {
				go walkFiles(cp.source.Path, cp.files, cp.fileCounter)
				go cp.bars.updateBar(cp.fileCounter, &cp.wg)

			} else {
				go walkFilesQuiet(cp.source.Path, cp.files)
			}
			cp.processFiles()

		} else if !isDir {
			// single file copy
			cp.copyFile(cp.source.Path)
		}
	}
	return err
}

func (cp *BucketCopier) copyS3ToFile() error {
	_, err := cp.checkPath(cp.dest.Path)

	if err == nil {

		withSize := false

		if !cp.quiet {
			withSize = true
			go cp.bars.updateBarListObjects(cp.srcLister.sizeChan, &cp.wg)
		} else {
			close(cp.sizeChan)
		}
		// List Objects
		go cp.srcLister.ListObjects(withSize)
		err = cp.downloadAllObjects()
	}
	return err
}

func (cp *BucketCopier) copy() error {

	var progress *mpb.Progress
	cp.wg.Add(1)

	if !cp.quiet {
		progress = cp.setupBars()
	}

	var err error

	if cp.source.Scheme == "s3" && cp.dest.Scheme == "s3" {
		// S3 to S3
		err = cp.copyS3ToS3()
	} else if cp.source.Scheme != "s3" {
		// Upload to S3
		err = cp.copyFileToS3()
	} else {
		// Download from S3
		err = cp.copyS3ToFile()
	}

	if err != nil {
		return err
	}

	if progress != nil {
		progress.Wait()
	} else {
		cp.wg.Wait()
	}
	return nil
}

// NewBucketCopier creates a new BucketCopier struct initialized with all variables needed to copy srcObjects in and out of
// a bucket
func NewBucketCopier(source string, dest string, threads int, quiet bool, sess *session.Session, template s3manager.UploadInput, destProfile string, recursive bool) (*BucketCopier, error) {

	var svc, destSvc *s3.S3
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

	var wg sync.WaitGroup
	if sourceURL.Scheme == "s3" {
		wg.Add(1)
		go func() {
			svc, err = checkBucket(sess, sourceURL.Host, &wg)
		}()
	}

	if destURL.Scheme == "s3" {

		if destProfile != "" {
			sess = session.Must(session.NewSessionWithOptions(session.Options{
				Profile:           destProfile,
				SharedConfigState: session.SharedConfigEnable,
				Config: aws.Config{
					CredentialsChainVerboseErrors: aws.Bool(true),
					MaxRetries:                    aws.Int(30),
				},
			}))
		}

		wg.Add(1)
		go func() {
			destSvc, err = checkBucket(sess, destURL.Host, &wg)
		}()
	}

	wg.Wait()
	if err != nil {
		return nil, err
	}

	if svc == nil {
		svc = destSvc
	}

	template.Bucket = aws.String(destURL.Host)

	bc := &BucketCopier{
		source:          *sourceURL,
		dest:            *destURL,
		recursive:       recursive,
		quiet:           quiet,
		uploadManager:   *s3manager.NewUploaderWithClient(svc),
		downloadManager: *s3manager.NewDownloaderWithClient(svc),
		svc:             svc,
		threads:         make(semaphore, threads),
		sizeChan:        make(chan objectCounter, threads),
		wg:              sync.WaitGroup{},
		template:        template,
	}

	if destProfile != "" {
		bc.uploadManager = *s3manager.NewUploaderWithClient(destSvc)
		bc.destSvc = destSvc
	}

	if sourceURL.Scheme == "s3" {
		bc.srcLister, err = NewBucketListerWithSvc(source, threads, svc)
		bc.srcObjects = make(chan []*s3.Object, threads)
		bc.srcLister.objects = bc.srcObjects
		bc.versions = make(chan []*s3.ObjectIdentifier, threads)
		bc.srcLister.versions = bc.versions
		bc.srcLister.threads = threads
		bc.srcLister.sizeChan = bc.sizeChan
	} else {
		bc.files = make(chan fileJob, bigChanSize)
		bc.fileCounter = make(chan int64, threads*2)
	}

	// Some logic to determine the base path to be used as the prefix for S3.  If the source pass ends with a "/" then
	// the base of the source path is not used in the S3 prefix as we assume iths the contents of the directory, not
	// the actual directory that is needed in the copy
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
