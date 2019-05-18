package main

import (
	"fmt"
	"io/ioutil"
	"os"

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
	rmRecursive   = rm.Flag("recursive", "Recurisvley delete").Short('r').Default("false").Bool()
	rmAllVersions = rm.Flag("all-versions", "Delete all versions").Default("false").Bool()
	rmPath        = rm.Arg("S3Uri", "S3 URL").Required().String()

	ls            = app.Command("ls", "list")
	lsAllVersions = ls.Flag("all-versions", "Delete all versions").Default("false").Bool()
	lsPath        = ls.Arg("S3Uri", "S3 URL").Required().String()

	cp            = app.Command("cp", "copy")
	cpSource      = cp.Arg("source", "file or s3 location").Required().String()
	cpDestination = cp.Arg("destination", "file or s3 location").Required().String()
	cpRecursive   = cp.Flag("recursive", "Recursively copy").Short('r').Default("False").Bool()
	cpConcurrent  = cp.Flag("concurrent", "Maximum number of concurrent uploads to S3.").Short('c').Default("50").Int()
	cpSSE         = cp.Flag("sse", "Specifies server-side encryption of the object in S3. Valid values are AES256 and aws:kms.").Default("AES256").Enum("AES256", "aws:kms")
	cpSSEKMSKeyId = cp.Flag("sse-kms-key-id", "The AWS KMS key ID that should be used to server-side encrypt the object in S3.").String()
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
)

//version variable which can be overidden at compile time
var (
	version = "dev-local-version"
	commit  = "none"
	date    = "unknown"
)

func main() {
	//Lets keep a track on how long things are taking us
	//startTime := time.Now()

	//Parse args and flags passed to us
	app.Version(version)
	kingpin.CommandLine.HelpFlag.Short('h')

	command := kingpin.MustParse(app.Parse(os.Args[1:]))

	logFile, _ := ioutil.TempFile(os.TempDir(), "s3kor")
	defer logFile.Close()

	var config zap.Config
	if *pVerbose {

		config = zap.NewDevelopmentConfig()

	} else {
		config = zap.NewProductionConfig()

	}

	config.OutputPaths = []string{
		logFile.Name(),
	}
	logger, _ := config.Build()
	zap.ReplaceGlobals(logger)
	zap.RedirectStdLog(logger)

	var sess *session.Session
	if *pProfile != "" {
		sess = session.Must(session.NewSessionWithOptions(session.Options{
			Profile:           *pProfile,
			SharedConfigState: session.SharedConfigEnable,
		}))

	} else {
		sess = session.Must(session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
		}))

	} //else
	if *pRegion != "" {
		sess.Config.Region = aws.String(*pRegion)
	}

	switch command {
	case rm.FullCommand():
		deleter, err := NewBucketDeleter(*rmPath, 50, *rmAllVersions, *rmRecursive, sess)
		if err != nil {
			fmt.Println(err.Error())
			logger.Fatal(err.Error())
		} else {
			deleter.delete()
		}
	case ls.FullCommand():
		lister, err := NewBucketLister(*lsPath, 50, sess)
		if err != nil {
			fmt.Println(err.Error())
			logger.Fatal(err.Error())
		} else {
			lister.list(*lsAllVersions)
		}
	case cp.FullCommand():

		inputTemplate := s3manager.UploadInput{
			ACL:                  cpACL,
			StorageClass:         cpStorageClass,
			ServerSideEncryption: cpSSE,
		}

		if *cpSSEKMSKeyId != "" {
			inputTemplate.ServerSideEncryption = cpSSEKMSKeyId
		}

		myCopier, err := NewBucketCopier(*cpSource, *cpDestination, *cpConcurrent, sess, inputTemplate)
		if err != nil {
			fmt.Println(err.Error())
			logger.Fatal(err.Error())
		} else {
			myCopier.copy(*cpRecursive)
		}
	}

}
