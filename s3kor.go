package main

import (
	"io/ioutil"
	"os"

	"github.com/aws/aws-sdk-go/aws"

	"go.uber.org/zap"

	"github.com/aws/aws-sdk-go/aws/session"
	"gopkg.in/alecthomas/kingpin.v2"
)

///Command line flags
var (
	app         = kingpin.New("s3kor", "s3 tools using golang concurency")
	pProfile    = app.Flag("profile", "AWS credentials/config file profile to use").String()
	pAutoRegion = app.Flag("auto-region", "Automatic region detection").Default("false").Bool()
	pRegion     = app.Flag("region", "AWS region").String()
	pVerbose    = app.Flag("verbose", "Verbose Logging").Default("false").Bool()

	rm            = app.Command("rm", "remove")
	rmRecursive   = rm.Flag("recursive", "Recurisvley delete").Default("false").Bool()
	rmAllVersions = rm.Flag("all-versions", "Delete all versions").Default("false").Bool()
	rmPath        = rm.Arg("S3Uri", "S3 URL").Required().String()

	ls            = app.Command("ls", "list")
	lsAllVersions = ls.Flag("all-versions", "Delete all versions").Default("false").Bool()
	lsPath        = ls.Arg("S3Uri", "S3 URL").Required().String()
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
		delete(sess, *rmPath, *pAutoRegion, *rmAllVersions, *rmRecursive)
	case ls.FullCommand():
		list(sess, *lsPath, *lsAllVersions)

	}
	//	}

	//if *pVersion {
	//	fmt.Println(version)
	//
	//} else {
	//	//Set up logging
	//	//var masterLogger *zap.Logger
	//
	//	var err error
	//	//if *pDebug {
	//	//	masterLogger, err = zap.NewDevelopment()
	//	//} else {
	//	//	masterLogger, err = zap.NewProduction()
	//	//
	//	//}
	//	if err == nil {
	//		//zap.ReplaceGlobals(masterLogger)
	//		//logger := zap.S()
	//		//defer logger.Sync()
	//		logger.Info("Starting s3thsync")
	//
	//		//Get your AWS credentials set
	//		var sess *session.Session
	//		if *pProfile != "" {
	//			logger.Debug("using passed profile")
	//			awskeyprofile := *pProfile
	//			sess = session.Must(session.NewSessionWithOptions(session.Options{
	//				Profile:           awskeyprofile,
	//				SharedConfigState: session.SharedConfigEnable,
	//			}))
	//
	//		} else {
	//			logger.Debug("using default provider chain")
	//			sess = session.Must(session.NewSessionWithOptions(session.Options{
	//				SharedConfigState: session.SharedConfigEnable,
	//			}))
	//
	//		} //else
	//
	//		//Workout what we have been asked to do
	//		if *pRemove != "" {
	//
	//			logger.Debug("Delete contents of bucket")
	//			delete(sess, rmPath, rmVersions)
	//
	//		}
	//		//else {
	//		//
	//		//	targetBucket := *pTarget
	//		//	targetBucket = targetBucket[5:len(targetBucket)]
	//		//
	//		//	manifestBucket := *pManifest
	//		//	if manifestBucket != "" {
	//		//		manifestBucket = manifestBucket[5:len(manifestBucket)]
	//		//	} //if
	//		//
	//		//	//Check to see if s3:// is used for source
	//		//	if strings.HasPrefix(*pSource, "s3://") {
	//		//
	//		//		logger.Debug("Copy from S3 to S3")
	//		//		sourceBucket := *pSource
	//		//		sourceBucket = sourceBucket[5:len(sourceBucket)]
	//		//		s3Copy(sess, sourceBucket, targetBucket)
	//		//
	//		//	} else {
	//		//		//File based source
	//		//
	//		//		info, err := os.Lstat(*pSource)
	//		//		if err != nil {
	//		//			logger.Fatal(err)
	//		//		} //if
	//		//		if !info.IsDir() {
	//		//			logger.Fatal(cwalk.ErrNotDir)
	//		//		} //if
	//		//
	//		//		if *pSync {
	//		//			logger.Debug("Sync file dir to S3")
	//		//			fileSync(*pSource, targetBucket, manifestBucket, sess, *pConcurent)
	//		//		} else {
	//		//			logger.Debug("Copy file dir to S3")
	//		//			fileCopy(*pSource, targetBucket, manifestBucket, *pConcurent, sess)
	//		//		} //else
	//		//	} //else
	//		//} //else
	//		endTime := time.Now()
	//		logger.Infof("Total run time: %s", endTime.Sub(startTime))
	//		fmt.Println("Total run time: ", endTime.Sub(startTime))
	//	} else {
	//		log.Fatal("Could not initiate logger, exiting")
	//	} //else
	//} //else
}
