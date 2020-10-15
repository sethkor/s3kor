package main

import (
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/vbauerster/mpb/v5"
	"github.com/vbauerster/mpb/v5/decor"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"go.uber.org/zap"
)

// copyerror stores the error and the input object that generated the error.  Since it could be one of 4 types of input
// we use pointers rather than having a different error struct for each input type
type copyError struct {
	error    error
	download *s3.GetObjectInput
	upload   *s3manager.UploadInput
	copy     *s3.CopyObjectInput
	multi    *MultiCopyInput
}

type copyErrorList struct {
	errorList []copyError
}

func (ce copyError) Error() string {
	var errString string

	if ce.download != nil {
		errString = *ce.download.Key + " "
	} else if ce.upload != nil {
		errString = *ce.upload.Key + " "
	} else if ce.copy != nil {
		errString = *ce.copy.Key + " "
	} else if ce.multi != nil {
		errString = *ce.multi.Input.Key + " "
	}

	return errString + ce.error.Error()
}

func (cel copyErrorList) Error() string {
	if len(cel.errorList) > 0 {
		out := make([]string, len(cel.errorList))
		for i, err := range cel.errorList {
			out[i] = err.Error()
		}
		return strings.Join(out, "\n")
	}
	return ""
}

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
	ewg             sync.WaitGroup
	files           chan fileJob
	fileCounter     chan int64
	//versions is not impleented yet
	//versions        chan []*s3.ObjectIdentifier
	srcObjects chan []*s3.Object
	sizeChan   chan objectCounter
	threads    semaphore
	template   s3manager.UploadInput
	srcLister  *BucketLister
	errors     chan copyError
	errorList  copyErrorList
}

// collectErrors processes any any errors passed via the error channel
// and stores them in the errorList
func (cp *BucketCopier) collectErrors() {
	defer cp.ewg.Done()
	for err := range cp.errors {
		cp.errorList.errorList = append(cp.errorList.errorList, err)
		//fmt.Println("ERROR: " + err.Error())
	}
}

func (cp *BucketCopier) updateBars(count int64, size int64, timeSince time.Duration) {
	if !cp.quiet {

		cp.bars.count.IncrInt64(count)
		if cp.bars.fileSize != nil {
			cp.bars.fileSize.IncrInt64(size)
		}
	}
}

func (cp *BucketCopier) copyFile(file string) {
	var logger = zap.S()

	f, err := os.Open(filepath.Clean(file))
	if err != nil {
		logger.Errorf("failed to open file %q, %v", file, err)

		if err != nil {
			cp.errors <- copyError{
				error: err,
			}
		}
	} else {
		// Upload the file to S3.
		input := cp.template
		input.Key = aws.String(cp.dest.Path + "/" + file[cp.sourceLength:])
		input.Body = f
		_, err = cp.uploadManager.Upload(&input)

		if err != nil {
			cp.errors <- copyError{
				error:  err,
				upload: &input,
			}
		}
		err = f.Close()
		if err != nil {
			cp.errors <- copyError{
				error: err,
			}
			return
		}
	}
}

func (cp *BucketCopier) uploadFile() func(file fileJob) {

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
					cp.errors <- copyError{
						error:  err,
						upload: &input,
					}
					return
				}
			}
		} else {
			cp.copyFile(file.path)
		}
		cp.updateBars(1, file.info.Size(), time.Since(start))
	}
}

func (cp *BucketCopier) processFiles() {
	defer cp.wg.Done()

	allThreads := cap(cp.threads)
	uploadFileFunc := cp.uploadFile()

	for file := range cp.files {
		cp.threads.acquire(1) // or block until one slot is free
		go uploadFileFunc(file)
	}
	cp.threads.acquire(allThreads) // don't continue until all goroutines complete
	close(cp.errors)

}

func (pb copyPb) updateBar(fileSize <-chan int64) {
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

func (pb copyPb) updateBarListObjects(fileSize <-chan objectCounter) {
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

func (cp *BucketCopier) downloadObjects() func(object *s3.Object) {
	var dirs sync.Map
	theSeparator := string(os.PathSeparator)

	return func(object *s3.Object) {
		defer cp.threads.release(1)
		start := time.Now()
		// Check File path and dir
		theFilePath := cp.dest.Path + theSeparator + *object.Key

		theDir := filepath.Dir(theFilePath)

		_, ok := dirs.Load(theDir)
		if !ok {

			_, err := os.Lstat(theDir)

			if err != nil {
				if os.IsNotExist(err) {
					err = os.MkdirAll(theDir, os.ModePerm)
				}
				if err != nil {
					cp.errors <- copyError{error: err}
					return
				}
			}

			dirs.Store(theDir, true)

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
			cp.errors <- copyError{error: err}
			return
		}

		downloadInput := s3.GetObjectInput{
			Bucket: aws.String(cp.source.Host),
			Key:    object.Key,
		}

		objectSize, err := cp.downloadManager.Download(theFile, &downloadInput)
		if err != nil {
			cp.errors <- copyError{
				error:    err,
				download: &downloadInput,
			}
			return
		}

		cp.updateBars(1, objectSize, time.Since(start))

		err = theFile.Close()
		if err != nil {
			cp.errors <- copyError{error: err}
			return
		}
	}
}

func (cp *BucketCopier) downloadAllObjects() {
	defer cp.wg.Done()
	downloadObjectsFunc := cp.downloadObjects()

	allThreads := cap(cp.threads)

	//we need one thread to update the progress bar and another to do the downloads

	for item := range cp.srcObjects {
		for _, object := range item {
			cp.threads.acquire(1)
			go downloadObjectsFunc(object)
		}

	}
	cp.threads.acquire(allThreads)
	close(cp.errors)

}

func (cp *BucketCopier) copyObjects() func(object *s3.Object) {

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

	return func(object *s3.Object) {
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
				cp.errors <- copyError{
					error: err,
					copy:  &copyInput,
				}
				return
			}

		} else {

			cmi := MultiCopyInput{
				Input:  &copyInput,
				Bucket: aws.String(cp.source.Host),
			}

			_, err := cm.Copy(&cmi)

			if err != nil {
				cp.errors <- copyError{
					error: err,
					multi: &cmi,
				}
				return
			}
		}

		cp.updateBars(1, *object.Size, time.Since(start))

	}
}

func (cp *BucketCopier) copyAllObjects() {
	defer cp.wg.Done()

	copyObjectsFunc := cp.copyObjects()

	allThreads := cap(cp.threads)

	//we need one thread to update the progress bar and another to do the downloads

	for item := range cp.srcObjects {
		for _, object := range item {
			cp.threads.acquire(1)
			go copyObjectsFunc(object)
		}
	}
	cp.threads.acquire(allThreads)
	close(cp.errors)
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
	return false, errors.New("The user-provided path " + path + " does not exsit")
}

func (cp *BucketCopier) copyS3ToS3(progress *mpb.Progress) {
	if !cp.quiet {
		go cp.bars.updateBarListObjects(cp.srcLister.sizeChan)
	}
	// List Objects
	go cp.srcLister.listObjects(true)

	//check to see if src and dest bucket are using the same credentials
	if cp.destSvc == cp.svc {
		cp.threads = make(semaphore, cp.srcLister.threads*1000)
		cp.copyAllObjects()
	} else {
		rp := newRemoteCopier(cp, progress)
		rp.remoteCopy()
	}
}

func (cp *BucketCopier) copyFileToS3() {
	isDir, err := cp.checkPath(cp.source.Path)

	if err != nil {
		cp.errors <- copyError{error: err}
		return
	}

	if cp.recursive {

		if !cp.quiet {
			go walkFiles(cp.source.Path, cp.files, cp.fileCounter)
			go cp.bars.updateBar(cp.fileCounter)

		} else {
			go walkFilesQuiet(cp.source.Path, cp.files)
		}
		cp.processFiles()

	} else {
		if !isDir {
			// single file copy
			cp.copyFile(cp.source.Path)

			if !cp.quiet {
				cp.bars.count.SetTotal(1, true)
				if cp.bars.fileSize != nil {
					info, _ := os.Lstat(cp.source.Path)
					cp.bars.fileSize.SetTotal(info.Size(), true)
				}
			}
		}
		close(cp.errors)
		if !cp.quiet {
			cp.wg.Done()
		}

	}
}

func (cp *BucketCopier) copyS3ToFile() {
	_, err := cp.checkPath(cp.dest.Path)

	if err != nil {
		cp.errors <- copyError{error: err}
		return
	}

	withSize := false

	if !cp.quiet {
		withSize = true
		go cp.bars.updateBarListObjects(cp.srcLister.sizeChan)
	} else {
		close(cp.sizeChan)
	}
	// List Objects
	go cp.srcLister.listObjects(withSize)
	cp.downloadAllObjects()
	cp.ewg.Wait()
}

func (cp *BucketCopier) copy() error {

	var progress *mpb.Progress
	cp.wg.Add(1)

	if !cp.quiet {
		progress = cp.setupBars()
	}

	var err error

	go cp.collectErrors()

	if cp.source.Scheme == "s3" && cp.dest.Scheme == "s3" {
		// S3 to S3
		cp.copyS3ToS3(progress)
	} else if cp.source.Scheme != "s3" {
		// Upload to S3
		cp.copyFileToS3()
	} else {
		// Download from S3
		cp.copyS3ToFile()
	}

	cp.wg.Wait()

	if progress != nil {

		if !cp.quiet {
			cp.bars.count.SetTotal(cp.bars.count.Current(), true)
			if cp.bars.fileSize != nil {
				cp.bars.fileSize.SetTotal(cp.bars.fileSize.Current(), true)
			}
		}
		progress.Wait()
	}

	cp.ewg.Wait()
	if len(cp.errorList.errorList) > 0 {
		err = cp.errorList
	}
	return err
}

// NewBucketCopier creates a new BucketCopier struct initialized with all variables needed to copy srcObjects in and out of
// a bucket
func NewBucketCopier(detectRegion bool, source string, dest string, threads int, quiet bool, sess *session.Session, template s3manager.UploadInput, destProfile string, recursive bool, accelerate bool) (*BucketCopier, error) {

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

	template.Bucket = aws.String(destURL.Host)

	cp := &BucketCopier{
		source:    *sourceURL,
		dest:      *destURL,
		recursive: recursive,
		quiet:     quiet,
		threads:   make(semaphore, threads),
		sizeChan:  make(chan objectCounter, threads),
		wg:        sync.WaitGroup{},
		template:  template,
		errors:    make(chan copyError, threads),
	}

	if cp.source.Scheme != "s3" {
		_, err = cp.checkPath(cp.source.Path)

		if err != nil {
			return nil, err
		}
	}

	if cp.dest.Scheme != "s3" {
		_, err = cp.checkPath(cp.dest.Path)

		if err != nil {
			return nil, err
		}
	}

	var destSvc *s3.S3
	var wg sync.WaitGroup
	if sourceURL.Scheme == "s3" {
		wg.Add(1)
		go func() {
			cp.svc, err = checkBucket(sess, detectRegion, cp.source.Host, &wg)
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
					S3UseAccelerate:               aws.Bool(accelerate),
				},
			}))
		}

		wg.Add(1)
		go func() {
			destSvc, err = checkBucket(sess, detectRegion, cp.dest.Host, &wg)
		}()
	}

	wg.Wait()
	if err != nil {
		return nil, err
	}

	if cp.svc == nil {
		cp.svc = destSvc
	}

	if cp.dest.Scheme == "s3" {
		if destProfile != "" {
			cp.uploadManager = *s3manager.NewUploaderWithClient(destSvc)
			cp.destSvc = destSvc
		} else {
			cp.uploadManager = *s3manager.NewUploaderWithClient(cp.svc)
			cp.destSvc = cp.svc
		}
	}

	if cp.source.Scheme == "s3" {
		cp.downloadManager = *s3manager.NewDownloaderWithClient(cp.svc)
		cp.srcLister, err = NewBucketListerWithSvc(source, false, threads, cp.svc)
		cp.srcObjects = make(chan []*s3.Object, threads)
		cp.srcLister.objects = cp.srcObjects
		cp.srcLister.threads = threads
		cp.srcLister.sizeChan = cp.sizeChan
	} else {
		cp.files = make(chan fileJob, bigChanSize)
		cp.fileCounter = make(chan int64, threads*2)
	}

	// Some logic to determine the base path to be used as the prefix for S3.  If the source pass ends with a "/" then
	// the base of the source path is not used in the S3 prefix as we assume this the contents of the directory, not
	// the actual directory that is needed in the copy
	_, splitFile := filepath.Split(cp.source.Path)
	includeRoot := 0
	if splitFile != "" {
		includeRoot = len(splitFile)
	}

	cp.sourceLength = len(cp.source.Path) - includeRoot
	if len(cp.source.Path) == 0 {
		cp.sourceLength++

	}

	cp.ewg.Add(1)
	return cp, nil
}
