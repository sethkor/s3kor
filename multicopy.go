package main

import (
	"fmt"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

// MaxCopyParts is the maximum allowed number of parts in a multi-part copy
// on Amazon S3.
const MaxCopyParts = 10000

// MinCopyPartSize is the minimum allowed part size when copying a part to
// Amazon S3.  Should be 5MB
const MinCopyPartSize int64 = 1024 * 1024 * 5

// MaxCopyPartSize is the maximum allowed part size when copying a part to
// Amazon S3.  Should be 5GB
const MaxCopyPartSize int64 = 1024 * 1024 * 1024 * 5

// DefaultCopyPartSize is the default part size to buffer chunkThreads of a
// payload into.
const DefaultCopyPartSize = MaxCopyPartSize

// DefaultCopyConcurrency is the default number of goroutines to spin up when
// using Copy().
const DefaultCopyConcurrency = 5

// A MultiCopyFailure wraps a failed S3 multipart copy. An error returned
// will satisfy this interface when a multi part copy failed to copy all
// chucks to S3. In the case of a failure the CopyID is needed to operate on
// the chunkThreads, if any, which were copyed.
//
// Example:
//
//     u := s3manager.NewCopyer(opts)
//     output, err := u.copy(input)
//     if err != nil {
//         if multierr, ok := err.(s3manager.MultiCopyFailure); ok {
//             // Process error and its associated uploadID
//             fmt.Println("Error:", multierr.Code(), multierr.Message(), multierr.CopyID())
//         } else {
//             // Process error generically
//             fmt.Println("Error:", err.Error())
//         }
//     }
//
type MultiCopyFailure interface {
	awserr.Error

	// Returns the copy id for the S3 multipart copy that failed.
	CopyID() string
}

// So that the Error interface type can be included as an anonymous field
// in the multiUploadError struct and not conflict with the error.Error() method.
type awsError awserr.Error

// A multiCopyError wraps the copy ID of a failed s3 multipart copy.
// Composed of BaseError for code, message, and original error
//
// Should be used for an error that occurred failing a S3 multipart copy,
// and a copy ID is available. If an uploadID is not available a more relevant
type multiCopyError struct {
	awsError

	// ID for multipart copy which failed.
	copyID string
}

// Error returns the string representation of the error.
//
// See apierr.BaseError ErrorWithExtra for output format
//
// Satisfies the error interface.
func (m multiCopyError) Error() string {
	extra := fmt.Sprintf("copy id: %s", m.copyID)
	return awserr.SprintError(m.Code(), m.Message(), extra, m.OrigErr())
}

// String returns the string representation of the error.
// Alias for Error to satisfy the stringer interface.
func (m multiCopyError) String() string {
	return m.Error()
}

// CopyID returns the id of the S3 copy which failed.
func (m multiCopyError) CopyID() string {
	return m.copyID
}

// CopyOutput represents a response from the Copy() call.
type CopyOutput struct {
	// The URL where the object was copyed to.
	Location string

	// The version of the object that was copyed. Will only be populated if
	// the S3 Bucket is versioned. If the bucket is not versioned this field
	// will not be set.
	VersionID *string

	// The ID for a multipart copy to S3. In the case of an error the error
	// can be cast to the MultiCopyFailure interface to extract the copy ID.
	CopyID string
}

// WithCopyerRequestOptions appends to the Copyer's API request options.
func WithCopyerRequestOptions(opts ...request.Option) func(*Copyer) {
	return func(u *Copyer) {
		u.RequestOptions = append(u.RequestOptions, opts...)
	}
}

// The Copyer structure that calls Copy(). It is safe to call Copy()
// on this structure for multiple srcObjects and across concurrent goroutines.
// Mutating the Copyer's properties is not safe to be done concurrently.
type Copyer struct {
	// The buffer size (in bytes) to use when buffering data into chunkThreads and
	// sending them as parts to S3. The minimum allowed part size is 5MB, and
	// if this value is set to zero, the DefaultCopyPartSize value will be used.
	PartSize int64

	// The number of goroutines to spin up in parallel per call to Copy when
	// sending parts. If this is set to zero, the DefaultCopyConcurrency value
	// will be used.
	//
	// The concurrency pool is not shared between calls to Copy.
	Concurrency int

	// Setting this value to true will cause the SDK to avoid calling
	// AbortMultipartCopy on a failure, leaving all successfully copyed
	// parts on S3 for manual recovery.
	//
	// Note that storing parts of an incomplete multipart copy counts towards
	// space usage on S3 and will add additional costs if not cleaned up.
	LeavePartsOnError bool

	// MaxCopyParts is the max number of parts which will be copyed to S3.
	// Will be used to calculate the partsize of the object to be copyed.
	// E.g: 5GB file, with MaxCopyParts set to 100, will copy the file
	// as 100, 50MB parts. With a limited of s3.MaxCopyParts (10,000 parts).
	//
	// MaxCopyParts must not be used to limit the total number of bytes copyed.
	// Use a type like to io.LimitReader (https://golang.org/pkg/io/#LimitedReader)
	// instead. An io.LimitReader is helpful when copying an unbounded reader
	// to S3, and you know its maximum size. Otherwise the reader's io.EOF returned
	// error must be used to signal end of stream.
	//
	// Defaults to package const's MaxCopyParts value.
	MaxCopyParts int

	// The client to use when copying to S3.
	S3 s3iface.S3API

	// List of request options that will be passed down to individual API
	// operation requests made by the copyer.
	RequestOptions []request.Option
}

// NewCopyer creates a new Copyer instance to copy srcObjects to S3. Pass In
// additional functional options to customize the copyer's behavior. Requires a
// client.ConfigProvider in order to create a S3 service client. The session.Session
// satisfies the client.ConfigProvider interface.
//
// Example:
//     // The session the S3 Copyer will use
//     sess := session.Must(session.NewSession())
//
//     // Create an copyer with the session and default options
//     copyer := s3manager.NewCopyer(sess)
//
//     // Create an copyer with the session and custom options
//     copyer := s3manager.NewCopyer(session, func(u *s3manager.Copyer) {
//          u.PartSize = 64 * 1024 * 1024 // 64MB per part
//     })
func NewCopyer(c client.ConfigProvider, options ...func(*Copyer)) *Copyer {
	u := &Copyer{
		S3:                s3.New(c),
		PartSize:          DefaultCopyPartSize,
		Concurrency:       DefaultCopyConcurrency,
		LeavePartsOnError: false,
		MaxCopyParts:      MaxCopyParts,
	}

	for _, option := range options {
		option(u)
	}

	return u
}

// NewCopyerWithClient creates a new Copyer instance to copy srcObjects to S3. Pass in
// additional functional options to customize the copyer's behavior. Requires
// a S3 service client to make S3 API calls.
//
// Example:
//     // The session the S3 Copyer will use
//     sess := session.Must(session.NewSession())
//
//     // S3 service client the Copy manager will use.
//     s3Svc := s3.New(sess)
//
//     // Create an copyer with S3 client and default options
//     copyer := s3manager.NewCopyerWithClient(s3Svc)
//
//     // Create an copyer with S3 client and custom options
//     copyer := s3manager.NewCopyerWithClient(s3Svc, func(u *s3manager.Copyer) {
//          u.PartSize = 64 * 1024 * 1024 // 64MB per part
//     })
func NewCopyerWithClient(svc s3iface.S3API, options ...func(*Copyer)) *Copyer {
	u := &Copyer{
		S3:                svc,
		PartSize:          DefaultCopyPartSize,
		Concurrency:       DefaultCopyConcurrency,
		LeavePartsOnError: false,
		MaxCopyParts:      MaxCopyParts,
	}

	for _, option := range options {
		option(u)
	}

	return u
}

// Copy copys an object to S3, intelligently buffering large files into
// smaller chunkThreads and sending them in parallel across multiple goroutines. You
// can configure the buffer size and concurrency through the Copyer's parameters.
//
// Additional functional options can be provided to configure the individual
// copy. These options are copies of the Copyer instance Copy is called from.
// Modifying the options will not impact the original Copyer instance.
//
// Use the WithCopyerRequestOptions helper function to pass in request
// options that will be applied to all API operations made with this copyer.
//
// It is safe to call this method concurrently across goroutines.
//
// Example:
//     // Copy input parameters
//     upParams := &s3manager.MultiCopyInput{
//         Bucket: &bucketName,
//         Key:    &keyName,
//         Body:   file,
//     }
//
//     // Perform an copy.
//     result, err := copyer.Copy(upParams)
//
//     // Perform copy with options different than the those in the Copyer.
//     result, err := copyer.Copy(upParams, func(u *s3manager.Copyer) {
//          u.PartSize = 10 * 1024 * 1024 // 10MB part size
//          u.LeavePartsOnError = true    // Don't delete the parts if the copy fails.
//     })
func (c Copyer) Copy(input *MultiCopyInput, options ...func(*Copyer)) (*CopyOutput, error) {
	return c.CopyWithContext(aws.BackgroundContext(), input, options...)
}

// CopyWithContext copys an object to S3, intelligently buffering large
// files into smaller chunkThreads and sending them in parallel across multiple
// goroutines. You can configure the buffer size and concurrency through the
// Copyer's parameters.
//
// CopyWithContext is the same as Copy with the additional support for
// Context input parameters. The Context must not be nil. A nil Context will
// cause a panic. Use the context to add deadlining, timeouts, etc. The
// CopyWithContext may create sub-contexts for individual underlying requests.
//
// Additional functional options can be provided to configure the individual
// copy. These options are copies of the Copyer instance Copy is called from.
// Modifying the options will not impact the original Copyer instance.
//
// Use the WithCopyerRequestOptions helper function to pass in request
// options that will be applied to all API operations made with this copyer.
//
// It is safe to call this method concurrently across goroutines.
func (c Copyer) CopyWithContext(ctx aws.Context, input *MultiCopyInput, opts ...func(*Copyer)) (*CopyOutput, error) {
	i := copyer{in: input, cfg: c, ctx: ctx}

	for _, opt := range opts {
		opt(&i.cfg)
	}
	i.cfg.RequestOptions = append(i.cfg.RequestOptions, request.WithAppendUserAgent("S3Manager"))

	return i.copy()
}

// CopyWithIterator will copy a batched amount of srcObjects to S3. This operation uses
// the iterator pattern to know which object to copy next. Since this is an interface this
// allows for custom defined functionality.
//
// Example:
//	svc:= s3manager.NewCopyer(sess)
//
//	srcObjects := []BatchCopyObject{
//		{
//			Object:	&s3manager.MultiCopyInput {
//				Key: aws.String("key"),
//				Bucket: aws.String("bucket"),
//			},
//		},
//	}
//
//	iter := &s3manager.CopyObjectsIterator{Objects: srcObjects}
//	if err := svc.CopyWithIterator(aws.BackgroundContext(), iter); err != nil {
//		return err
//	}
//func (u Copyer) CopyWithIterator(ctx aws.Context, iter BatchCopyIterator, opts ...func(*Copyer)) error {
//	var errs []Error
//	for iter.Next() {
//		object := iter.CopyObject()
//		if _, err := u.CopyWithContext(ctx, object.Object, opts...); err != nil {
//			s3Err := Error{
//				OrigErr: err,
//				Bucket:  object.Object.Bucket,
//				Key:     object.Object.Key,
//			}
//
//			errs = append(errs, s3Err)
//		}
//
//		if object.After == nil {
//			continue
//		}
//
//		if err := object.After(); err != nil {
//			s3Err := Error{
//				OrigErr: err,
//				Bucket:  object.Object.Bucket,
//				Key:     object.Object.Key,
//			}
//
//			errs = append(errs, s3Err)
//		}
//	}
//
//	if len(errs) > 0 {
//		return NewBatchError("BatchedCopyIncomplete", "some srcObjects have failed to copy.", errs)
//	}
//	return nil
//}

// internal structure to manage a copy to S3.
type copyer struct {
	ctx aws.Context
	cfg Copyer

	in *MultiCopyInput

	readerPos int64 // current reader position
	totalSize int64 // set to -1 if the size is not known
	parts     int64

	head *s3.HeadObjectOutput

	bufferPool sync.Pool
}

// internal logic for deciding whether to copy a single part or use a
// multipart copy.
func (c *copyer) copy() (*CopyOutput, error) {
	c.init()

	if c.cfg.PartSize <= MinCopyPartSize {
		msg := fmt.Sprintf("part size must be at least %d bytes", MinCopyPartSize)
		return nil, awserr.New("ConfigError", msg, nil)
	}

	ho := s3.HeadObjectInput{
		Bucket: c.in.Bucket,
		Key:    c.in.Input.Key,
	}
	//awsutil.Copy(&ho, c.in.Input)

	// do a head object to get all information we need to copy in parts
	var err error
	c.head, err = c.cfg.S3.HeadObject(&ho)

	//Number of parts is part size divided by object length + 1
	c.parts = ((*c.head.ContentLength) / c.cfg.PartSize) + 1

	if err != nil {
		return nil, awserr.New("HeadObject", "head object failed", err)
	}

	mu := multicopyer{copyer: c}
	return mu.copy()
}

// init will initialize all default options.
func (c *copyer) init() {
	if c.cfg.Concurrency == 0 {
		c.cfg.Concurrency = DefaultCopyConcurrency
	}
	if c.cfg.PartSize == 0 {
		c.cfg.PartSize = DefaultCopyPartSize
	}
	if c.cfg.MaxCopyParts == 0 {
		c.cfg.MaxCopyParts = MaxCopyParts
	}

	c.bufferPool = sync.Pool{
		New: func() interface{} { return make([]byte, c.cfg.PartSize) },
	}
}

// internal structure to manage a specific multipart copy to S3.
type multicopyer struct {
	*copyer
	wg       sync.WaitGroup
	m        sync.Mutex
	err      error
	uploadID string
	parts    completedParts
}

// keeps track of a single chunk of data being sent to S3.
type copyChunk struct {
	start  int64
	finish int64
	num    int64
}

// completedParts is a wrapper to make parts sortable by their part number,
// since S3 required this list to be sent in sorted order.
type completedParts []*s3.CompletedPart

func (a completedParts) Len() int           { return len(a) }
func (a completedParts) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a completedParts) Less(i, j int) bool { return *a[i].PartNumber < *a[j].PartNumber }

// copy will perform a multipart copy using the firstBuf buffer containing
// the first chunk of data.
func (u *multicopyer) copy() (*CopyOutput, error) {

	params := &s3.CreateMultipartUploadInput{}
	awsutil.Copy(params, u.in.Input)

	// Create the multipart
	resp, err := u.cfg.S3.CreateMultipartUploadWithContext(u.ctx, params, u.cfg.RequestOptions...)
	if err != nil {
		return nil, err
	}
	u.uploadID = *resp.UploadId

	// Create the workers
	ch := make(chan copyChunk, u.cfg.Concurrency)
	for i := 0; i < u.cfg.Concurrency; i++ {
		u.wg.Add(1)
		go u.readChunk(ch)
	}

	// Send part 1 to the workers
	var num int64 = 1
	//ch <- chunk{buf: firstBuf, part: firstPart, num: num}
	var first int64
	last := u.copyer.cfg.PartSize
	for i := int64(0); i < u.copyer.parts; i++ {

		// This copy exceeded maximum number of supported parts, error now.
		if num > int64(u.cfg.MaxCopyParts) || num > int64(MaxCopyParts) {
			var msg string
			if num > int64(u.cfg.MaxCopyParts) {
				msg = fmt.Sprintf("exceeded total allowed configured MaxCopyParts (%d). Adjust PartSize to fit in this limit",
					u.cfg.MaxCopyParts)
			} else {
				msg = fmt.Sprintf("exceeded total allowed S3 limit MaxCopyParts (%d). Adjust PartSize to fit in this limit",
					MaxCopyParts)
			}
			u.seterr(awserr.New("TotalPartsExceeded", msg, nil))
			break
		}

		if last >= *u.head.ContentLength {
			last = *u.head.ContentLength - 1
		}
		ch <- copyChunk{start: first, finish: last, num: i + 1}
		first = last + 1
		last = last + u.copyer.cfg.PartSize
	}

	// Close the channel, wait for workers, and complete copy
	close(ch)
	u.wg.Wait()
	complete := u.complete()

	if err := u.geterr(); err != nil {
		return nil, &multiCopyError{
			awsError: awserr.New(
				"MultipartCopy",
				"copy multipart failed",
				err),
			copyID: u.uploadID,
		}
	}

	// Create a presigned URL of the S3 Get Object in order to have parity with
	// single part copy.
	getReq, _ := u.cfg.S3.GetObjectRequest(&s3.GetObjectInput{
		Bucket: u.in.Input.Bucket,
		Key:    u.in.Input.Key,
	})
	getReq.Config.Credentials = credentials.AnonymousCredentials
	copyLocation, _, _ := getReq.PresignRequest(1)

	return &CopyOutput{
		Location:  copyLocation,
		VersionID: complete.VersionId,
		CopyID:    u.uploadID,
	}, nil
}

// readChunk runs in worker goroutines to pull chunkThreads off of the ch channel
// and send() them as CopyPart requests.
func (u *multicopyer) readChunk(ch chan copyChunk) {
	defer u.wg.Done()
	for {
		data, ok := <-ch

		if !ok {
			break
		}

		if u.geterr() == nil {
			if err := u.send(data); err != nil {
				u.seterr(err)
			}
		}
	}
}

// send performs an CopyPart request and keeps track of the completed
// part information.
func (u *multicopyer) send(c copyChunk) error {
	params := &s3.UploadPartCopyInput{
		Bucket:               u.in.Input.Bucket,
		Key:                  u.in.Input.Key,
		CopySource:           u.in.Input.CopySource,
		CopySourceRange:      aws.String(fmt.Sprintf("bytes=%d-%d", c.start, c.finish)),
		UploadId:             &u.uploadID,
		SSECustomerAlgorithm: u.in.Input.SSECustomerAlgorithm,
		SSECustomerKey:       u.in.Input.SSECustomerKey,
		PartNumber:           &c.num,
	}
	resp, err := u.cfg.S3.UploadPartCopyWithContext(u.ctx, params, u.cfg.RequestOptions...)

	if err != nil {
		return err
	}

	n := c.num
	completed := &s3.CompletedPart{ETag: resp.CopyPartResult.ETag, PartNumber: &n}

	u.m.Lock()
	u.parts = append(u.parts, completed)
	u.m.Unlock()

	return nil
}

// geterr is a thread-safe getter for the error object
func (u *multicopyer) geterr() error {
	u.m.Lock()
	defer u.m.Unlock()

	return u.err
}

// seterr is a thread-safe setter for the error object
func (u *multicopyer) seterr(e error) {
	u.m.Lock()
	defer u.m.Unlock()

	u.err = e
}

// fail will abort the multipart unless LeavePartsOnError is set to true.
func (u *multicopyer) fail() {
	if u.cfg.LeavePartsOnError {
		return
	}

	params := &s3.AbortMultipartUploadInput{
		Bucket:   u.in.Input.Bucket,
		Key:      u.in.Input.Key,
		UploadId: &u.uploadID,
	}
	_, err := u.cfg.S3.AbortMultipartUploadWithContext(u.ctx, params, u.cfg.RequestOptions...)
	if err != nil {
		logMessage(u.cfg.S3, aws.LogDebug, fmt.Sprintf("failed to abort multipart copy, %v", err))
	}
}

// complete successfully completes a multipart copy and returns the response.
func (u *multicopyer) complete() *s3.CompleteMultipartUploadOutput {
	if u.geterr() != nil {
		u.fail()
		return nil
	}

	// Parts must be sorted in PartNumber order.
	sort.Sort(u.parts)

	params := &s3.CompleteMultipartUploadInput{
		Bucket:          u.in.Input.Bucket,
		Key:             u.in.Input.Key,
		UploadId:        &u.uploadID,
		MultipartUpload: &s3.CompletedMultipartUpload{Parts: u.parts},
	}
	resp, err := u.cfg.S3.CompleteMultipartUploadWithContext(u.ctx, params, u.cfg.RequestOptions...)
	if err != nil {
		u.seterr(err)
		u.fail()
	}

	return resp
}

func logMessage(svc s3iface.S3API, level aws.LogLevelType, msg string) {
	s, ok := svc.(*s3.S3)
	if !ok {
		return
	}

	if s.Config.Logger == nil {
		return
	}

	if s.Config.LogLevel.Matches(level) {
		s.Config.Logger.Log(msg)
	}
}
