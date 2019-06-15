package main

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"runtime"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/aws/aws-sdk-go/aws"

	"go.uber.org/zap"

	"github.com/aws/aws-sdk-go/aws/session"
	"gopkg.in/alecthomas/kingpin.v2"
)

///Command line flags
var (
	app      = kingpin.New("s3kor", "s3 tools using golang concurency")
	pProfile = app.Flag("profile", "AWS credentials/config file profile to use").String()
	pRegion  = app.Flag("region", "AWS region").String()
	pVerbose = app.Flag("verbose", "Verbose Logging").Default("false").Bool()

	rm            = app.Command("rm", "remove")
	rmQuiet       = rm.Flag("quiet", "Does not display the operations performed from the specified command.").Short('q').Default("false").Bool()
	rmRecursive   = rm.Flag("recursive", "Recurisvley delete").Short('r').Default("false").Bool()
	rmAllVersions = rm.Flag("all-versions", "Delete all versions and delete markers").Default("false").Bool()
	rmPath        = rm.Arg("S3Uri", "S3 URL").Required().String()

	ls            = app.Command("ls", "list")
	lsAllVersions = ls.Flag("all-versions", "List all versions").Default("false").Bool()
	lsPath        = ls.Arg("S3Uri", "S3 URL").Required().String()

	cp            = app.Command("cp", "copy")
	cpSource      = cp.Arg("source", "file or s3 location").Required().String()
	cpDestination = cp.Arg("destination", "file or s3 location").Required().String()
	cpQuiet       = cp.Flag("quiet", "Does not display the operations performed from the specified command.").Short('q').Default("false").Bool()
	cpRecursive   = cp.Flag("recursive", "Recursively copy").Short('r').Default("False").Bool()
	cpConcurrent  = cp.Flag("concurrent", "Maximum number of concurrent uploads to S3.").Short('c').Default("50").Int()
	cpSSE         = cp.Flag("sse", "Specifies server-side encryption of the object in S3. Valid values are AES256 and aws:kms.").Default("AES256").Enum("AES256", "aws:kms")
	cpSSEKMSKeyID = cp.Flag("sse-kms-key-id", "The AWS KMS key ID that should be used to server-side encrypt the object in S3.").String()
	cpACL         = cp.Flag("acl", "Object ACL").Default(s3.ObjectCannedACLPrivate).Enum(s3.ObjectCannedACLAuthenticatedRead,
		s3.ObjectCannedACLAwsExecRead,
		s3.ObjectCannedACLBucketOwnerFullControl,
		s3.ObjectCannedACLBucketOwnerRead,
		s3.ObjectCannedACLPrivate,
		s3.ObjectCannedACLPublicRead,
		s3.ObjectCannedACLPublicReadWrite)
	cpStorageClass = cp.Flag("storage-class", "Storage Class").Default(s3.StorageClassStandard).Enum(s3.StorageClassStandard,
		s3.StorageClassStandardIa,
		s3.StorageClassDeepArchive,
		s3.StorageClassGlacier,
		s3.StorageClassOnezoneIa,
		s3.StorageClassReducedRedundancy,
		s3.StorageClassIntelligentTiering)
	cpDestProfile = cp.Flag("dest-profile", "Destination bucket AWS credentials/config file profile to use if different from --profile").String()
)

//version variable which can be overidden at compile time
var (
	version = "dev-local-version"
	commit  = "none"
	date    = "unknown"
)

///Needed to workaround abug with zap logger and daft windows file paths/names
func newWinFileSink(u *url.URL) (zap.Sink, error) {
	// Remove leading slash left by url.Parse()
	return os.OpenFile(u.Path[1:], os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644) //nolint:gosec
}

func setUpLogger() {
	var config zap.Config
	if *pVerbose {

		config = zap.NewDevelopmentConfig()

	} else {
		config = zap.NewProductionConfig()

	}

	logFile, err := ioutil.TempFile(os.TempDir(), "s3kor")

	if err != nil {
		defer logFile.Close()

		//workaround for windows file paths and names

		if runtime.GOOS == "windows" {
			err = zap.RegisterSink("winfile", newWinFileSink)
			if err == nil {

				config.OutputPaths = []string{
					"winfile:///" + logFile.Name(),
				}
			}
		} else {
			config.OutputPaths = []string{
				logFile.Name(),
			}
		}
	}

	logger, err := config.Build()
	if err != nil {
		zap.ReplaceGlobals(logger)
		zap.RedirectStdLog(logger)
	}

}

func main() {
	//Lets keep a track on how long things are taking us
	//startTime := time.Now()

	//Parse args and flags passed to us
	app.Version(version)
	kingpin.CommandLine.HelpFlag.Short('h')

	command := kingpin.MustParse(app.Parse(os.Args[1:]))

	setUpLogger()
	logger := zap.S()

	var sess *session.Session
	if *pProfile != "" {

		sess = session.Must(session.NewSessionWithOptions(session.Options{
			Profile:           *pProfile,
			SharedConfigState: session.SharedConfigEnable,
			Config: aws.Config{
				CredentialsChainVerboseErrors: aws.Bool(true),
				MaxRetries:                    aws.Int(30),
			},
		}))

	} else {
		sess = session.Must(session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
			Config: aws.Config{
				CredentialsChainVerboseErrors: aws.Bool(true),
				MaxRetries:                    aws.Int(30),
			},
		}))
	} //else

	if *pRegion != "" {
		sess.Config.Region = aws.String(*pRegion)
	}

	switch command {
	case rm.FullCommand():
		deleter, err := NewBucketDeleter(*rmPath, *rmQuiet, 50, *rmAllVersions, *rmRecursive, sess)
		if err != nil {
			fmt.Println(err.Error())
			logger.Fatal(err.Error())
		} else {
			deleter.delete(*rmAllVersions)
		}
	case ls.FullCommand():
		lister, err := NewBucketLister(*lsPath, 50, sess)
		if err != nil {
			fmt.Println(err.Error())
			logger.Fatal(err.Error())
		} else {
			lister.List(*lsAllVersions)
		}
	case cp.FullCommand():

		inputTemplate := s3manager.UploadInput{
			ACL:                  cpACL,
			StorageClass:         cpStorageClass,
			ServerSideEncryption: cpSSE,
		}

		if *cpSSEKMSKeyID != "" {
			inputTemplate.ServerSideEncryption = cpSSEKMSKeyID
		}

		myCopier, err := NewBucketCopier(*cpSource, *cpDestination, *cpConcurrent, *cpQuiet, sess, inputTemplate, *cpDestProfile)
		if err != nil {
			fmt.Println(err.Error())
			logger.Fatal(err.Error())
		} else {
			myCopier.copy(*cpRecursive)
		}
	}

}
