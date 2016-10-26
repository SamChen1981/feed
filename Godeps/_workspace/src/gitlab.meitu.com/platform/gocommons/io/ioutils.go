// Package io contains common io util.
package io

import (
	"os"
	"path"
	"path/filepath"
)

// IsFileExist checks whether specified file is existed
func IsFileExist(path string) (exists bool, err error) {
	_, err = os.Stat(path)
	exists = err == nil || os.IsExist(err)
	return
}

// EnsureDirectory make sure that the specified dir is existed or created.
func EnsureDirectory(targetDir string) (resultPath string, isValid bool) {
	var (
		targetPath string
		err        error
	)

	if path.IsAbs(targetDir) {
		targetPath = targetDir
	} else {
		targetPath, err = filepath.Abs(targetDir)
		if err != nil {
			return targetDir, false
		}
	}

	if _, err = os.Stat(targetPath); os.IsNotExist(err) {
		err = os.MkdirAll(targetPath, 0755)
		if err != nil {
			return targetDir, false
		}
	}

	return targetPath, true
}
