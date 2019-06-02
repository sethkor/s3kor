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

// BucketCopier stores everything we need to copy objects to or from a bucket
type BucketCopier struct {
	source          url.URL
	target          url.URL
	quiet           bool
	sourceLength    int
	uploadManager   s3manager.Uploader
	downloadManager s3manager.Downloader
	svc             s3.S3
	bars            copyPb
	wg              sync.WaitGroup
	files           chan fileJob
	fileCounter     chan int64
	versions        chan []*s3.ObjectIdentifier
	objects         chan []*s3.Object
	sizeChan        chan objectCounter
	threads         semaphore
	template        s3manager.UploadInput
	lister          *BucketLister
}

func (cp *BucketCopier) copyFile(file string) {
	var logger = zap.S()

	f, err := os.Open(filepath.Clean(file))
	if err != nil {
		logger.Errorf("failed to open file %q, %v", file, err)
	} else {
		// Upload the file to S3.
		input := cp.template
		input.Key = aws.String(cp.target.Path + "/" + file[cp.sourceLength:])
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
				input.Key = aws.String(cp.target.Path + "/" + file.path[cp.sourceLength:] + "/")
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
		if !cp.quiet {
			cp.bars.count.IncrInt64(1)
			cp.bars.fileSize.IncrInt64(file.info.Size(), time.Since(start))
		}
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

}

func (pb copyPb) updateBar(fileSize <-chan int64, wg *sync.WaitGroup) {
	defer wg.Done()
	var fileCount int64
	var fileSizeTotal int64

	for size := range fileSize {
		fileCount++
		fileSizeTotal += size
		pb.count.SetTotal(fileCount, false)
		pb.fileSize.SetTotal(fileSizeTotal, false)
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
		pb.fileSize.SetTotal(fileSizeTotal, false)
	}
}

func (cp *BucketCopier) downloadObjects() (func(object *s3.Object) error, error) {
	var logger = zap.S()

	var dirs sync.Map
	theSeparator := string(os.PathSeparator)

	return func(object *s3.Object) error {
		defer cp.threads.release(1)
		// Check File path and dir
		theFilePath := cp.target.Path + theSeparator + *object.Key

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

		if !cp.quiet {
			cp.bars.count.Increment()
			cp.bars.fileSize.IncrInt64(objectSize)
		}

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

	for item := range cp.objects {

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
		Bucket:               aws.String(cp.target.Host),
		Key:                  aws.String(cp.target.Path),
		ServerSideEncryption: cp.template.ServerSideEncryption,
	}

	if *cp.template.ServerSideEncryption != "" {
		copyTemplate.ServerSideEncryption = cp.template.ServerSideEncryption
	}
	return func(object *s3.Object) error {
		defer cp.threads.release(1)

		if *object.Size < maxCopySize {

			copyInput := copyTemplate

			copyInput.CopySource = aws.String(cp.source.Host + "/" + *object.Key)

			if cp.source.Path[len(cp.source.Path)-1:] == "/" {
				copyInput.Key = aws.String(cp.target.Path + "/" + (*object.Key)[len(cp.source.Path)-1:])
			} else {
				copyInput.Key = aws.String(cp.target.Path + "/" + (*object.Key))
			}

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
		}

		if !cp.quiet {
			cp.bars.count.Increment()
			cp.bars.fileSize.IncrInt64(*object.Size)
		}
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

	for item := range cp.objects {

		for _, object := range item {
			cp.threads.acquire(1)
			go copyObjectsFunc(object)
		}

	}
	cp.threads.acquire(allThreads)

	if !cp.quiet {
		cp.bars.count.SetTotal(cp.bars.count.Current(), true)
		cp.bars.fileSize.SetTotal(cp.bars.fileSize.Current(), true)
	}

	return nil
}

func (cp *BucketCopier) copy(recursive bool) {

	if cp.source.Scheme == "s3" && cp.target.Scheme == "s3" {

		var progress *mpb.Progress

		cp.wg.Add(1)

		if !cp.quiet {

			progress = mpb.New(mpb.WithWaitGroup(&cp.wg))

			cp.bars.count = progress.AddBar(0,
				mpb.PrependDecorators(
					// simple name decorator
					decor.Name("Files", decor.WC{W: 6, C: decor.DSyncWidth}),
					decor.CountersNoUnit(" %d / %d", decor.WCSyncWidth),
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
				),
			)
			go cp.bars.updateBarListObjects(cp.lister.sizeChan, &cp.wg)
		}
		// List Objects
		go cp.lister.ListObjects(true)

		cp.copyAllObjects()

		if progress != nil {
			progress.Wait()
		} else {
			cp.wg.Wait()
		}

	} else if cp.source.Scheme != "s3" {

		path, err := filepath.Abs(cp.source.Path)
		if err != nil {
			fmt.Printf("The user-provided path %s does not exsit\n", cp.source.Path)
			return
		}
		info, err := os.Lstat(path)
		if err != nil {
			fmt.Printf("The user-provided path %s does not exsit\n", cp.source.Path)
			return
		}
		if recursive {
			if !info.IsDir() {
				fmt.Printf("The user-provided path %s/ does not exsit\n", cp.source.Path)
				return
			}

			var progress *mpb.Progress

			if !cp.quiet {
				go walkFiles(cp.source.Path, cp.files, cp.fileCounter)
				cp.wg.Add(1)
				progress = mpb.New(mpb.WithWaitGroup(&cp.wg))

				cp.bars.count = progress.AddBar(0,
					mpb.PrependDecorators(
						// simple name decorator
						decor.Name("Files", decor.WC{W: 6, C: decor.DSyncWidth}),
						decor.CountersNoUnit(" %d / %d", decor.WCSyncWidth),
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
					),
				)

				go cp.bars.updateBar(cp.fileCounter, &cp.wg)

			} else {
				go walkFilesQuiet(cp.source.Path, cp.files)
			}
			cp.wg.Add(1)
			go cp.processFiles()

			if progress != nil {
				progress.Wait()
			} else {
				// cp.wg.Add(1)
				cp.wg.Wait()
			}
		} else if !info.IsDir() {
			// single file copy
			cp.copyFile(cp.source.Path)

		}
	} else {
		// Download from S3

		path, err := filepath.Abs(cp.target.Path)
		if err != nil {
			fmt.Printf("The user-provided path %s does not exsit\n", cp.target.Path)
			return
		}
		info, err := os.Lstat(path)
		if err != nil {
			fmt.Printf("The user-provided path %s does not exsit\n", cp.target.Path)
			return
		}

		if !info.IsDir() {
			fmt.Printf("The user-provided path %s/ does not exsit\n", cp.target.Path)
			return
		}

		var progress *mpb.Progress

		cp.wg.Add(1)

		withSize := false

		if !cp.quiet {
			withSize = true
			progress = mpb.New(mpb.WithWaitGroup(&cp.wg))

			cp.bars.count = progress.AddBar(0,
				mpb.PrependDecorators(
					// simple name decorator
					decor.Name("Files", decor.WC{W: 6, C: decor.DSyncWidth}),
					decor.CountersNoUnit(" %d / %d", decor.WCSyncWidth),
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
				),
			)
			go cp.bars.updateBarListObjects(cp.lister.sizeChan, &cp.wg)
		} else {
			close(cp.sizeChan)
		}
		// List Objects
		go cp.lister.ListObjects(withSize)
		cp.downloadAllObjects()

		if progress != nil {
			progress.Wait()
		} else {
			cp.wg.Wait()
		}
		if err != nil {
			fmt.Println(err)

		}

	}

}

// NewBucketCopier creates a new BucketCopier struct initialized with all variables needed to copy objects in and out of
// a bucket
func NewBucketCopier(source string, dest string, threads int, quiet bool, sess *session.Session, template s3manager.UploadInput) (*BucketCopier, error) {

	var svc *s3.S3
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
		source:          *sourceURL,
		target:          *destURL,
		quiet:           quiet,
		uploadManager:   *s3manager.NewUploaderWithClient(svc),
		downloadManager: *s3manager.NewDownloaderWithClient(svc),
		svc:             *svc,
		threads:         make(semaphore, threads),
		sizeChan:        make(chan objectCounter, threads),
		wg:              sync.WaitGroup{},
		template:        template,
	}

	if sourceURL.Scheme == "s3" {
		bc.lister, err = NewBucketLister(source, threads, sess)
		//if destURL.Scheme == "s3" {
		bc.objects = make(chan []*s3.Object, threads)
		bc.lister.objects = bc.objects
		//} else {
		bc.versions = make(chan []*s3.ObjectIdentifier, threads)
		bc.lister.versions = bc.versions
		//}

		bc.lister.threads = threads
		bc.lister.sizeChan = bc.sizeChan
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
