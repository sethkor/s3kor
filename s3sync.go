package main

import (
	"errors"
	"sync"
	"time"

	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Syncer stores everything we need to sync srcObjects to or from a bucket
type Syncer struct {
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
func (sy *Syncer) obj2Map(wg *sync.WaitGroup) {
	defer wg.Done()
	for objSlice := range sy.destObjects {
		for _, obj := range objSlice {
			sy.destMap[*obj.Key] = objDateSize{obj.LastModified, obj.Size}
		}
	}
}

// file2Map takes the result of a file walk places them into the destmap so that they can be compared
// with the source of the sync
func (sy *Syncer) file2Map(wg *sync.WaitGroup) {
	defer wg.Done()
	for file := range sy.files {
		modTime := file.info.ModTime()
		size := file.info.Size()
		sy.destMap[file.path] = objDateSize{&modTime, &size}
	}
}

func (sy *Syncer) syncS3ToS3() error {
	defer sy.wg.Done()

	// Start listing the destination bucket first thing.  drop the results into a map so we can compare later
	go sy.destLister.ListObjects(false)
	var wg sync.WaitGroup
	wg.Add(1)
	go sy.obj2Map(&wg)

	if !sy.quiet {

		go sy.bars.updateBarListObjects(sy.srcLister.sizeChan, &sy.wg)
	}

	//list the source objects now, these drive the UI
	go sy.srcLister.ListObjects(true)

	copyObjectsFunc, err := sy.copyObjects()

	if err != nil {
		return err
	}
	allThreads := cap(sy.threads)

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

func (sy *Syncer) syncFileToS3() error {
	defer sy.wg.Done()

	// Start listing the destination bucket first thing.  Drop the results into a map so we can compare later
	go sy.destLister.ListObjects(false)
	var wg sync.WaitGroup
	wg.Add(1)
	go sy.obj2Map(&wg)

	_, err := sy.checkPath(sy.source.Path)

	if err != nil {
		return err
	}

	if !sy.quiet {
		go walkFiles(sy.source.Path, sy.files, sy.fileCounter)
		sy.wg.Add(1)

		go sy.bars.updateBar(sy.fileCounter, &sy.wg)

	} else {
		go walkFilesQuiet(sy.source.Path, sy.files)
	}

	uploadFileFunc := sy.uploadFile()

	allThreads := cap(sy.threads)

	//wait here until the destination listing is complete
	wg.Wait()

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

func (sy *Syncer) syncObjToFile() (func(item []*s3.Object), error) {

	downloadFileFunc, err := sy.downloadObjects()

	if err != nil {
		return nil, err
	}

	var basePath string
	if len(sy.dest.Path) != 0 {
		basePath = sy.dest.Path
		if sy.dest.Path[len(sy.dest.Path)-1:] != "/" {
			basePath = basePath + "/"
		}
	}

	return func(item []*s3.Object) {
		defer sy.threads.release(1)
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
	}, nil
}

func (sy *Syncer) syncS3ToFile() error {
	defer sy.wg.Done()

	_, err := sy.checkPath(sy.dest.Path)
	if err != nil {
		return err
	}

	go walkFilesQuiet(sy.dest.Path, sy.files)
	var wg sync.WaitGroup
	wg.Add(1)
	go sy.file2Map(&wg)

	if sy.quiet {
		close(sy.sizeChan)
	}

	// List Objects
	go sy.srcLister.ListObjects(false)

	allThreads := cap(sy.threads)

	var total int64 = 1

	syncObjToFileFunc, err := sy.syncObjToFile()

	wg.Wait()

	for item := range sy.srcObjects {
		total = total + int64(len(item))

		if !sy.quiet {
			sy.bars.count.SetTotal(total, false)
		}
		sy.threads.acquire(1)
		go syncObjToFileFunc(item)

	}

	sy.threads.acquire(allThreads)

	if !sy.quiet {
		sy.bars.count.SetTotal(total-1, true)
	}
	return nil
}

func (sy *Syncer) setupBars() *mpb.Progress {
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

func (sy *Syncer) sync() error {

	var progress *mpb.Progress
	sy.wg.Add(1)

	if !sy.quiet {
		progress = sy.setupBars()
	}

	var err error
	if sy.source.Scheme == "s3" && sy.dest.Scheme == "s3" {
		//S3 to S3
		err = sy.syncS3ToS3()

	} else if sy.source.Scheme != "s3" {
		/// Sync to S3
		err = sy.syncFileToS3()
	} else {
		// Sync from S3
		err = sy.syncS3ToFile()
	}
	if err != nil {
		return err
	}

	if progress != nil {
		progress.Wait()
	} else {
		sy.wg.Wait()
	}
	return nil
}

// NewSync creates a new Syncer struct initialized with all variables needed to sync files and objects in and out of
// a bucket
func NewSync(source string, dest string, threads int, quiet bool, sess *session.Session, template s3manager.UploadInput, destProfile string) (*Syncer, error) {

	sy := &Syncer{
		destMap: make(map[string]objDateSize),
	}

	var err error
	sy.BucketCopier, err = NewBucketCopier(source, dest, threads, quiet, sess, template, destProfile, true)

	if err != nil {
		if sy.source.Scheme != "s3" && sy.dest.Scheme != "s3" {
			return nil, errors.New("usage: aws s3 sync <LocalPath> <S3Uri> or <S3Uri> <LocalPath> or <S3Uri> <S3Uri>")

		}
		return nil, err
	}

	sy.files = make(chan fileJob, bigChanSize)

	// Some BucketCopier attributes not used for syncing We should zero them so the gc frees
	// them up

	sy.versions = nil
	if sy.srcLister != nil {
		sy.srcLister.versions = nil
	}

	// Thee attributes are specific to a syncer
	if sy.dest.Scheme == "s3" {
		sy.destLister, err = NewBucketListerWithSvc(dest, threads, sy.svc)
		if err != nil {
			return nil, err
		}
		sy.destObjects = make(chan []*s3.Object, threads)
		sy.destLister.objects = sy.destObjects
	}
	return sy, nil
}
