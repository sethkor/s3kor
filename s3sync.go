package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"go.uber.org/zap"

	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// BucketCopier stores everything we need to copy srcObjects to or from a bucket
type Syncer struct {
	source          url.URL
	target          url.URL
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
	srcObjects      chan []*s3.Object
	destObjects     chan []*s3.Object
	sizeChan        chan objectCounter
	threads         semaphore
	template        s3manager.UploadInput
	srcLister       *BucketLister
	destLister      *BucketLister

	destMap map[string]objDateSize
}

type objDateSize struct {
	lastModified *time.Time
	size         *int64
}

func (sy *Syncer) copyObjects() (func(object *s3.Object) error, error) {
	var logger = zap.S()

	copyTemplate := s3.CopyObjectInput{
		ACL:                  sy.template.ACL,
		Bucket:               aws.String(sy.target.Host),
		Key:                  aws.String(sy.target.Path),
		ServerSideEncryption: sy.template.ServerSideEncryption,
	}

	if *sy.template.ServerSideEncryption != "" {
		copyTemplate.ServerSideEncryption = sy.template.ServerSideEncryption
	}
	cm := NewCopyerWithClient(sy.svc)

	return func(object *s3.Object) error {
		defer sy.threads.release(1)

		copyInput := copyTemplate

		copyInput.CopySource = aws.String(sy.source.Host + "/" + *object.Key)

		if len(sy.source.Path) == 0 {
			copyInput.Key = object.Key
		} else if sy.source.Path[len(sy.source.Path)-1:] == "/" {
			copyInput.Key = aws.String(sy.target.Path + "/" + (*object.Key)[len(sy.source.Path)-1:])
		} else {
			copyInput.Key = aws.String(sy.target.Path + "/" + (*object.Key))
		}

		if *object.Size <= MaxCopyPartSize {

			_, err := sy.uploadManager.S3.CopyObject(&copyInput)

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
				Bucket: aws.String(sy.source.Host),
			}

			_, err := cm.Copy(&cmi)

			if err != nil {
				return err
			}

		}
		if !sy.quiet {
			sy.bars.count.Increment()
		}
		return nil
	}, nil
}

func (sy *Syncer) syncAllObjectsS3() error {
	defer sy.wg.Done()

	copyObjectsFunc, err := sy.copyObjects()

	if err != nil {
		return err
	}
	allThreads := cap(sy.threads)
	if !sy.quiet {
		fmt.Printf("0")
	}

	//we need one thread to update the progress bar and another to do the downloads

	for item := range sy.srcObjects {

		for _, object := range item {
			copyObj := true
			if details, ok := sy.destMap[*object.Key]; ok {
				if details.lastModified.After(*object.LastModified) && *details.size == *object.Size {
					copyObj = false
				}
			}
			if copyObj {
				sy.threads.acquire(1)
				go copyObjectsFunc(object)
			} else {
				if !sy.quiet {
					sy.bars.count.Increment()
				}
			}
		}
	}
	sy.threads.acquire(allThreads)

	return nil
}

func (sy *Syncer) obj2Map(wg *sync.WaitGroup) {
	defer wg.Done()
	for objSlice := range sy.destObjects {
		for _, obj := range objSlice {
			sy.destMap[*obj.Key] = objDateSize{obj.LastModified, obj.Size}
		}
	}
}

func (sy *Syncer) file2Map(wg *sync.WaitGroup) {
	defer wg.Done()
	for file := range sy.files {
		modTime := file.info.ModTime()
		size := file.info.Size()
		sy.destMap[file.path] = objDateSize{&modTime, &size}
	}
}

func (sy *Syncer) copyFile(file string) {
	var logger = zap.S()

	f, err := os.Open(filepath.Clean(file))
	if err != nil {
		logger.Errorf("failed to open file %q, %v", file, err)
	} else {
		// Upload the file to S3.
		input := sy.template
		input.Key = aws.String(sy.target.Path + "/" + file[sy.sourceLength:])
		input.Body = f
		_, err = sy.uploadManager.Upload(&input)

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

func (sy *Syncer) uploadFile() func(file fileJob) {
	var logger = zap.S()

	return func(file fileJob) {
		defer sy.threads.release(1)
		input := sy.template
		if file.info.IsDir() {
			// Don't create a prefix for the base dir
			if len(file.path) != sy.sourceLength {
				input.Key = aws.String(sy.target.Path + "/" + file.path[sy.sourceLength:] + "/")
				_, err := sy.uploadManager.Upload(&input)

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
			sy.copyFile(file.path)
		}
		if !sy.quiet {
			sy.bars.count.IncrInt64(1)
		}
	}
}

func (sy *Syncer) syncToS3() error {
	defer sy.wg.Done()

	uploadFileFunc := sy.uploadFile()

	allThreads := cap(sy.threads)
	if !sy.quiet {
		fmt.Printf("0")
	}

	//we need one thread to update the progress bar and another to do the downloads

	for file := range sy.files {
		copyFile := true
		if details, ok := sy.destMap[file.path[sy.sourceLength:]]; ok {
			if details.lastModified.After(file.info.ModTime()) && *details.size == file.info.Size() {
				copyFile = false
			}
		}
		if copyFile {
			sy.threads.acquire(1)
			go uploadFileFunc(file)
		} else {
			if !sy.quiet {
				sy.bars.count.IncrInt64(1)
			}
		}

	}
	sy.threads.acquire(allThreads)

	return nil
}

func (sy *Syncer) downloadObjects() (func(object *s3.Object) error, error) {
	var logger = zap.S()

	var dirs sync.Map
	theSeparator := string(os.PathSeparator)

	return func(object *s3.Object) error {
		defer sy.threads.release(1)
		// Check File path and dir
		theFilePath := sy.target.Path + theSeparator + *object.Key

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
			Bucket: aws.String(sy.source.Host),
			Key:    object.Key,
		}

		_, err = sy.downloadManager.Download(theFile, &downloadInput)

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

		err = theFile.Close()
		if err != nil {
			logger.Error(err)
		}
		if !sy.quiet {
			sy.bars.count.IncrInt64(1)
		}
		return err
	}, nil
}

func (sy *Syncer) syncFromS3() error {
	defer sy.wg.Done()

	downloadFileFunc, err := sy.downloadObjects()

	if err != nil {
		return err
	}

	allThreads := cap(sy.threads)
	if !sy.quiet {
		fmt.Printf("0")
	}

	var basePath string
	if len(sy.target.Path) != 0 {
		basePath = sy.target.Path
		if sy.target.Path[len(sy.target.Path)-1:] != "/" {
			basePath = basePath + "/"
		}
	}

	var total int64 = 1
	for item := range sy.srcObjects {
		total = total + int64(len(item))
		sy.bars.count.SetTotal(total, false)

		for _, object := range item {
			copyObj := true
			if details, ok := sy.destMap[basePath+(*object.Key)]; ok {
				if details.lastModified.After(*object.LastModified) {
					if *details.size == *object.Size {
						copyObj = false
					}
				}
			}
			if copyObj {
				sy.threads.acquire(1)
				go downloadFileFunc(object)
			} else {
				if !sy.quiet {
					sy.bars.count.IncrInt64(1)
				}
			}
		}

	}

	sy.threads.acquire(allThreads)
	sy.bars.count.SetTotal(total-1, true)
	return nil
}

func (sy *Syncer) setupBars() *mpb.Progress {
	sy.wg.Add(1)
	progress := mpb.New(mpb.WithWaitGroup(&sy.wg))

	sy.bars.count = progress.AddBar(0,
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

	return progress
}

func (sy *Syncer) sync() {

	var progress *mpb.Progress

	if sy.source.Scheme == "s3" && sy.target.Scheme == "s3" {

		// Start listing the destination bucket first thing.  drop the results into a map so we can compare later
		go sy.destLister.ListObjects(false)
		var wg sync.WaitGroup
		wg.Add(1)
		go sy.obj2Map(&wg)

		if !sy.quiet {

			progress = sy.setupBars()

			go sy.bars.updateBarListObjects(sy.srcLister.sizeChan, &sy.wg)
		}

		//list the source objects now, these drive the UI
		go sy.srcLister.ListObjects(true)

		//wait here until the destination listing is complete
		wg.Wait()

		//Now begin processing what we have listed
		sy.syncAllObjectsS3()

		if progress != nil {
			progress.Wait()
		} else {
			sy.wg.Wait()
		}

	} else if sy.source.Scheme != "s3" {

		/// Sync to S3

		// Start listing the destination bucket first thing.  drop the results into a map so we can compare later
		go sy.destLister.ListObjects(false)
		var wg sync.WaitGroup
		wg.Add(1)
		go sy.obj2Map(&wg)

		path, err := filepath.Abs(sy.source.Path)
		if err != nil {
			fmt.Printf("The user-provided path %s does not exsit\n", sy.source.Path)
			return
		}
		_, err = os.Lstat(path)
		if err != nil {
			fmt.Printf("The user-provided path %s does not exsit\n", sy.source.Path)
			return
		}

		var progress *mpb.Progress

		if !sy.quiet {
			go walkFiles(sy.source.Path, sy.files, sy.fileCounter)
			sy.wg.Add(1)

			progress = sy.setupBars()

			go sy.bars.updateBar(sy.fileCounter, &sy.wg)

		} else {
			go walkFilesQuiet(sy.source.Path, sy.files)
		}

		//wait here until the destination listing is complete
		wg.Wait()

		sy.syncToS3()

		if progress != nil {
			progress.Wait()
		} else {
			sy.wg.Wait()
		}

	} else {
		// Download from S3

		path, err := filepath.Abs(sy.target.Path)
		if err != nil {
			fmt.Printf("The user-provided path %s does not exsit\n", sy.target.Path)
			return
		}
		info, err := os.Lstat(path)
		if err != nil {
			fmt.Printf("The user-provided path %s does not exsit\n", sy.target.Path)
			return
		}

		if !info.IsDir() {
			fmt.Printf("The user-provided path %s/ does not exsit\n", sy.target.Path)
			return
		}

		go walkFilesQuiet(sy.target.Path, sy.files)
		var wg sync.WaitGroup
		wg.Add(1)
		go sy.file2Map(&wg)

		if !sy.quiet {
			progress = sy.setupBars()

		} else {
			close(sy.sizeChan)
		}
		// List Objects
		go sy.srcLister.ListObjects(false)

		wg.Wait()

		sy.syncFromS3()

		if progress != nil {
			progress.Wait()
		} else {
			sy.wg.Wait()
		}
		if err != nil {
			fmt.Println(err)

		}

	}

}

// NewBucketCopier creates a new BucketCopier struct initialized with all variables needed to copy srcObjects in and out of
// a bucket
func NewSync(source string, dest string, threads int, quiet bool, sess *session.Session, template s3manager.UploadInput, destProfile string) (*Syncer, error) {

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
		return nil, errors.New("usage: aws s3 sync <LocalPath> <S3Uri> or <S3Uri> <LocalPath> or <S3Uri> <S3Uri>")

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

	sy := &Syncer{
		source:          *sourceURL,
		target:          *destURL,
		quiet:           quiet,
		uploadManager:   *s3manager.NewUploaderWithClient(svc),
		downloadManager: *s3manager.NewDownloaderWithClient(svc),
		svc:             svc,
		destSvc:         destSvc,
		threads:         make(semaphore, threads),
		sizeChan:        make(chan objectCounter, threads),
		wg:              sync.WaitGroup{},
		template:        template,
		destMap:         make(map[string]objDateSize),
	}

	if destProfile != "" {
		sy.uploadManager = *s3manager.NewUploaderWithClient(destSvc)
	}

	sy.files = make(chan fileJob, bigChanSize)

	if sourceURL.Scheme == "s3" {
		sy.srcLister, err = NewBucketListerWithSvc(source, threads, svc)
		sy.srcObjects = make(chan []*s3.Object, threads)
		sy.srcLister.objects = sy.srcObjects
		sy.srcLister.threads = threads
		sy.srcLister.sizeChan = sy.sizeChan
	} else {

		sy.fileCounter = make(chan int64, threads*2)
	}

	if destURL.Scheme == "s3" {
		sy.destLister, err = NewBucketListerWithSvc(dest, threads, svc)
		sy.destObjects = make(chan []*s3.Object, threads)
		sy.destLister.objects = sy.destObjects
	}

	// Some logic to determine the base path to be used as the prefix for S3.  If the source pass ends with a "/" then
	// the base of the source path is not used in the S3 prefix as we assume iths the contents of the directory, not
	// the actual directory that is needed in the copy
	_, splitFile := filepath.Split(sy.source.Path)
	includeRoot := 0
	if splitFile != "" {
		includeRoot = len(splitFile)
	}

	sy.sourceLength = len(sy.source.Path) - includeRoot
	if len(sy.source.Path) == 0 {
		sy.sourceLength++

	}
	return sy, nil
}
