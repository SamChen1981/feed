package log

import (
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sync"

	"github.com/cihub/seelog"
	"gitlab.meitu.com/platform/gocommons"
	"gitlab.meitu.com/platform/gocommons/io"
	"gopkg.in/fsnotify.v1"
)

const (
	seelogCfgFileNamePre = "seelog-"
	seelogCfgFileNameSuf = ".xml"
)

type seelogWrapper struct {
	name        string
	configFile  string
	rwMutex     *sync.RWMutex
	stackDepth  int
	innerLogger seelog.LoggerInterface
}

var pid int

func init() {
	//seelog，每个配置名称都是一个单独的配置文件
	loggerCreator = CreateSeelogger

	pid = os.Getpid()
	err := seelog.RegisterCustomFormatter("PID", createPIDFormatter)
	if err != nil {
		fmt.Printf("seelog:ERROR failed to register custom PID formatter: %s\n", err.Error())
	}
}

func CreateSeelogger(name string) (Logger, error) {
	configFile := name
	if !filepath.IsAbs(name) {
		configFile = path.Join(gocommons.ConfPath, seelogCfgFileNamePre+name+seelogCfgFileNameSuf)
	}
	if exists, _ := io.IsFileExist(configFile); exists == true {
		logger, err := seelog.LoggerFromConfigAsFile(configFile)
		if err != nil {
			return nil, err
		}

		logInstance := &seelogWrapper{
			name:        name,
			configFile:  configFile,
			rwMutex:     &sync.RWMutex{},
			innerLogger: logger,
		}
		logInstance.watchConfig()

		return logInstance, nil
	}
	return nil, fmt.Errorf("seelog:WARN No config could be found for logger (%s %s).", name, configFile)
}

func (logger *seelogWrapper) replaceLogger(newLogger seelog.LoggerInterface) error {
	if newLogger == nil {
		return errors.New("new logger can not be nil")
	}

	logger.rwMutex.Lock()
	defer logger.rwMutex.Unlock()

	defer func() {
		if err := recover(); err != nil {
			fmt.Errorf("recovered from panic during ReplaceLogger: %s", err)
		}
	}()

	if logger.innerLogger != nil && !logger.innerLogger.Closed() {
		logger.innerLogger.Flush()
		logger.innerLogger.Close()
	}

	logger.innerLogger = newLogger

	return nil
}

func (logger *seelogWrapper) watchConfig() {
	go func() {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			fmt.Printf("create seelog config file watcher failed: %v\n", err)
		}
		defer watcher.Close()

		// we have to watch the entire directory to pick up renames/atomic saves in a cross-platform way
		configFile := filepath.Clean(logger.configFile)
		// convert file path to absolute path
		configFile, _ = filepath.EvalSymlinks(configFile)
		configDir, file := filepath.Split(configFile)
		configDir, _ = filepath.EvalSymlinks(configDir)
		configFile = path.Join(configDir, file)

		var (
			contents []byte
			fileHash []byte
		)
		hasher := md5.New()
		// 检查文件内容, 为空则继续下一个事件检查
		if contents, err = ioutil.ReadFile(configFile); err != nil {
			logger.Errorf("read seelog configuration %v content failed: %v", configFile, err)
		} else if len(contents) > 0 {
			hasher.Reset()
			fileHash = hasher.Sum(contents)
		}

		done := make(chan bool)
		go func() {
			var currentHash []byte
			for {
				select {
				case event := <-watcher.Events:
					// we only care about the config file
					if filepath.Clean(event.Name) == configFile &&
						(event.Op&fsnotify.Create == fsnotify.Create ||
							event.Op&fsnotify.Write == fsnotify.Write) {

						if contents, err = ioutil.ReadFile(configFile); err != nil {
							logger.Errorf("seelog configuration %v read content failed: %v",
								configFile, err)
							continue
						} else if len(contents) == 0 {
							logger.Infof("seelog configuration %v is empty", configFile)
							continue
						}

						hasher.Reset()
						currentHash = hasher.Sum(contents)
						// check file contents change
						if bytes.Equal(fileHash, currentHash) {
							continue
						}

						logger.Infof(
							"seelog configuration %v was modified! reloading...",
							event.Name,
						)

						newLogger, err := seelog.LoggerFromConfigAsBytes(contents)
						if err != nil {
							logger.Errorf("create seelog from file %v failed: %v",
								configFile, err)
							continue
						}

						newLogger.SetAdditionalStackDepth(logger.stackDepth)
						if err = logger.replaceLogger(newLogger); err == nil {
							fileHash = currentHash
							logger.Infof(
								"seelog configuration %v reload successfully!",
								configFile,
							)
						} else {
							logger.Errorf("seelog configuration %v reload failed: %v",
								configFile, err)
						}
					}
				case err := <-watcher.Errors:
					fmt.Printf("watch seelog config file error: %v\n", err)
				}
			}
		}()

		watcher.Add(configDir)
		<-done
	}()
}

func (logger *seelogWrapper) Debug(v ...interface{}) {
	logger.rwMutex.RLock()
	defer logger.rwMutex.RUnlock()
	logger.innerLogger.Debug(v)
}
func (logger *seelogWrapper) Info(v ...interface{}) {
	logger.rwMutex.RLock()
	defer logger.rwMutex.RUnlock()
	logger.innerLogger.Info(v)
}
func (logger *seelogWrapper) Warn(v ...interface{}) {
	logger.rwMutex.RLock()
	defer logger.rwMutex.RUnlock()
	logger.innerLogger.Warn(v)
}
func (logger *seelogWrapper) Error(v ...interface{}) {
	logger.rwMutex.RLock()
	defer logger.rwMutex.RUnlock()
	logger.innerLogger.Error(v)
}
func (logger *seelogWrapper) Fatal(v ...interface{}) {
	logger.rwMutex.RLock()
	logger.innerLogger.Critical(v)
	logger.rwMutex.RUnlock()
	logger.innerLogger.Flush()
	os.Exit(-1)
}

func (logger *seelogWrapper) Debugf(format string, params ...interface{}) {
	logger.rwMutex.RLock()
	defer logger.rwMutex.RUnlock()
	logger.innerLogger.Debugf(format, params...)
}
func (logger *seelogWrapper) Infof(format string, params ...interface{}) {
	logger.rwMutex.RLock()
	defer logger.rwMutex.RUnlock()
	logger.innerLogger.Infof(format, params...)
}
func (logger *seelogWrapper) Errorf(format string, params ...interface{}) {
	logger.rwMutex.RLock()
	defer logger.rwMutex.RUnlock()
	logger.innerLogger.Errorf(format, params...)
}
func (logger *seelogWrapper) Warnf(format string, params ...interface{}) {
	logger.rwMutex.RLock()
	defer logger.rwMutex.RUnlock()
	logger.innerLogger.Warnf(format, params...)
}
func (logger *seelogWrapper) Fatalf(format string, params ...interface{}) {
	logger.rwMutex.RLock()
	logger.innerLogger.Criticalf(format, params...)
	logger.rwMutex.RUnlock()
	logger.innerLogger.Flush()
	os.Exit(-1)
}

func (logger *seelogWrapper) Flush() {
	logger.rwMutex.RLock()
	defer logger.rwMutex.RUnlock()
	logger.innerLogger.Flush()
}

func (logger *seelogWrapper) SetAdditionalStackDepth(depth int) {
	logger.rwMutex.Lock()
	defer logger.rwMutex.Unlock()
	logger.stackDepth = depth
	logger.innerLogger.SetAdditionalStackDepth(depth)
}

func createPIDFormatter(params string) seelog.FormatterFunc {
	return func(message string, level seelog.LogLevel, context seelog.LogContextInterface) interface{} {
		return pid
	}
}
