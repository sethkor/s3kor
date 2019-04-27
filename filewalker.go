package main

import (
	"os"
	"path/filepath"

	"github.com/iafan/cwalk"
	"go.uber.org/zap"
)

//filejob struct is needed as we are using channels and need not only the file information but also the path when
//copying the files
type fileJob struct {
	path string
	info os.FileInfo
}

//upLoadFile uses a channel in order to facilitate walking the file system and copying files in parallel which we can do
//when doing a full copy as opposed to a sync where we need to compare source and destination time stamps
func upLoadFile(files chan<- fileJob, fileSize chan<- int64) filepath.WalkFunc {
	return func(path string, info os.FileInfo, err error) error {
		if err != nil {
			zap.S().Error(err)
			return err
		}

		//We are only interested in regular files
		if info.Mode().IsRegular() {
			files <- fileJob{
				path: path,
				info: info,
			}
			fileSize <- info.Size()
		}
		return nil
	}
}

func walkFiles(sourceDir string, files chan<- fileJob, filecount chan<- int64) {
	var logger = zap.S()
	logger.Debugf("Walking the source directory path")

	err := cwalk.Walk(sourceDir, upLoadFile(files, filecount))

	if err != nil {
		logger.Error(err)
	}
	close(filecount)
	close(files)
	logger.Debug("Finished file walk")
}
