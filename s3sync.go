package main

import (
	"errors"
	"sync"
	"time"

	"github.com/vbauerster/mpb/v5"
	"github.com/vbauerster/mpb/v5/decor"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// BucketSyncer stores everything we need to sync srcObjects to or from a bucket
type BucketSyncer struct {
	*BucketCopier // we reuse a lot of the BucketCopier structs and methods to get things done
	destObjects   chan []*s3.Object
	destLister    *BucketLister
	destMap       map[string]objDateSize //map where the key is the absolute path and the value is objDateSize
}

// objDateSize stores mod time and sizes to use when checking if a file or object should by synced
type objDateSize struct {
	lastModified *time.Time
	size         *int64
}

// obj2Map takes the result of listing objects in S3 and places them into the destmap so that they can be compared
// with the source of the sync
func (sy *BucketSyncer) obj2Map(wg *sync.WaitGroup) {
	defer wg.Done()
	for objSlice := range sy.destObjects {
		for _, obj := range objSlice {
			sy.destMap[*obj.Key] = objDateSize{obj.LastModified, obj.Size}
		}
	}
}

// file2Map takes the result of a file walk places them into the destmap so that they can be compared
// with the source of the sync
func (sy *BucketSyncer) file2Map(wg *sync.WaitGroup) {
	defer wg.Done()
	for file := range sy.files {
		modTime := file.info.ModTime()
		size := file.info.Size()
		sy.destMap[file.path] = objDateSize{&modTime, &size}
	}
}

func (sy *BucketSyncer) syncS3ToS3() {
	defer sy.wg.Done()

	// Start listing the destination bucket first thing.  drop the results into a map so we can compare later
	go sy.destLister.listObjects(false)
	var wg sync.WaitGroup
	wg.Add(1)
	go sy.obj2Map(&wg)

	if !sy.quiet {

		go sy.bars.updateBarListObjects(sy.srcLister.sizeChan)
	}

	//list the source objects now, these drive the UI
	go sy.srcLister.listObjects(true)

	copyObjectsFunc := sy.copyObjects()

	if sy.destSvc == sy.svc {
		sy.threads = make(semaphore, sy.srcLister.threads*1000)
	}

	allThreads := cap(sy.threads)

	rp := newRemoteCopier(sy.BucketCopier, nil)

	//wait here until the destination listing is complete
	wg.Wait()
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
				if sy.destSvc == sy.svc {
					go copyObjectsFunc(object)
				} else {
					go rp.remoteCopyObject(object)
				}

			} else {
				if !sy.quiet {
					sy.bars.count.Increment()
				}
			}
		}
	}
	sy.threads.acquire(allThreads)
	close(sy.errors)
}

func (sy *BucketSyncer) syncFileToS3() {
	defer sy.wg.Done()

	// Start listing the destination bucket first thing.  Drop the results into a map so we can compare later
	go sy.destLister.listObjects(false)
	var wg sync.WaitGroup
	wg.Add(1)
	go sy.obj2Map(&wg)

	_, err := sy.checkPath(sy.source.Path)

	if err != nil {
		sy.errors <- copyError{error: err}
		return
	}

	if !sy.quiet {
		go walkFiles(sy.source.Path, sy.files, sy.fileCounter)

		go sy.bars.updateBar(sy.fileCounter)

	} else {
		go walkFilesQuiet(sy.source.Path, sy.files)
	}

	uploadFileFunc := sy.uploadFile()

	allThreads := cap(sy.threads)

	//wait here until the destination listing is complete
	wg.Wait()
	for file := range sy.files {
		copyFile := true
		if details, ok := sy.destMap[file.path]; ok {
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
	close(sy.errors)
}

func (sy *BucketSyncer) syncObjToFile(wg *sync.WaitGroup) func(item []*s3.Object) {

	downloadFileFunc := sy.downloadObjects()

	var basePath string
	if len(sy.dest.Path) != 0 {
		basePath = sy.dest.Path
		if sy.dest.Path[len(sy.dest.Path)-1:] != "/" {
			basePath = basePath + "/"
		}
	}

	return func(item []*s3.Object) {
		defer wg.Done()

		for _, object := range item {
			copyObj := true
			if details, ok := sy.destMap[*object.Key]; ok {
				if details.lastModified.After(*object.LastModified) {
					if *details.size == *object.Size {
						copyObj = false
					}
				}
			}
			if copyObj {
				sy.threads.acquire(1)
				downloadFileFunc(object)
			} else {
				if !sy.quiet {
					sy.bars.count.IncrInt64(1)
				}
			}
		}
	}
}

func (sy *BucketSyncer) syncS3ToFile() {
	defer sy.wg.Done()

	_, err := sy.checkPath(sy.dest.Path)
	if err != nil {
		sy.errors <- copyError{error: err}
		return
	}

	go walkFilesQuiet(sy.dest.Path, sy.files)
	var wg sync.WaitGroup
	wg.Add(1)
	go sy.file2Map(&wg)

	if sy.quiet {
		close(sy.sizeChan)
	}

	// List Objects
	go sy.srcLister.listObjects(false)

	var total int64 = 1

	syncObjToFileFunc := sy.syncObjToFile(&wg)

	wg.Wait()

	for item := range sy.srcObjects {
		total = total + int64(len(item))

		if !sy.quiet {
			sy.bars.count.SetTotal(total, false)
		}
		wg.Add(1)
		go syncObjToFileFunc(item)

	}

	if !sy.quiet {
		sy.bars.count.SetTotal(total-1, false)
	}
	wg.Wait()
	close(sy.errors)

}

func (sy *BucketSyncer) setupBars() *mpb.Progress {
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

func (sy *BucketSyncer) sync() error {

	var progress *mpb.Progress
	sy.wg.Add(1)

	if !sy.quiet {
		progress = sy.setupBars()
	}

	var err error
	go sy.collectErrors()

	if sy.source.Scheme == "s3" && sy.dest.Scheme == "s3" {
		//S3 to S3
		sy.syncS3ToS3()
	} else if sy.source.Scheme != "s3" {
		/// Sync to S3
		sy.syncFileToS3()
	} else {
		// Sync from S3
		sy.syncS3ToFile()
	}

	sy.wg.Wait()

	if progress != nil {
		if !sy.quiet {
			sy.bars.count.SetTotal(sy.bars.count.Current(), true)
		}
		progress.Wait()
	}

	sy.ewg.Wait()
	if len(sy.errorList.errorList) > 0 {
		err = sy.errorList
	}
	return err
}

// NewSync creates a new BucketSyncer struct initialized with all variables needed to sync files and objects in and out of
// a bucket
func NewSync(detectRegion bool, source string, dest string, threads int, quiet bool, sess *session.Session, template s3manager.UploadInput, destProfile string, accelerate bool) (*BucketSyncer, error) {

	sy := &BucketSyncer{
		destMap: make(map[string]objDateSize),
	}

	var err error
	sy.BucketCopier, err = NewBucketCopier(detectRegion, source, dest, threads, quiet, sess, template, destProfile, true, accelerate)

	if err != nil {
		if sy.source.Scheme != "s3" && sy.dest.Scheme != "s3" {
			return nil, errors.New("usage: aws s3 sync <LocalPath> <S3Uri> or <S3Uri> <LocalPath> or <S3Uri> <S3Uri>")

		}
		return nil, err
	}

	sy.files = make(chan fileJob, bigChanSize)

	// Some BucketCopier attributes not used for syncing We should zero them so the gc frees
	// them up

	//versions is not implemented yet
	//sy.versions = nil
	if sy.srcLister != nil {
		sy.srcLister.versions = nil
	}

	// Thee attributes are specific to a syncer
	if sy.dest.Scheme == "s3" {
		sy.destLister, err = NewBucketListerWithSvc(dest, false, threads, sy.destSvc)
		if err != nil {
			return nil, err
		}
		sy.destObjects = make(chan []*s3.Object, threads)
		sy.destLister.objects = sy.destObjects
	}
	return sy, nil
}
