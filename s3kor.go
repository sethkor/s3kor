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
		list(sess, *lsPath, *pAutoRegion, *lsAllVersions)

	}


}
