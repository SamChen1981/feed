package log

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// Console logger对象
// 一般是提供测试mock对象
type consoleLogger struct {
	mutex     sync.Mutex // ensures atomic writes;
	CallDepth int        //获取几个层级的调用行号，如果为0则不输出行号 print callerInfo if 1
}

var ConsoleLogger = &consoleLogger{}

func (cl *consoleLogger) Debug(args ...interface{}) {
	cl.output("DEBUG", fmt.Sprint(args...))
}
func (cl *consoleLogger) Info(args ...interface{}) {
	cl.output("INFO", fmt.Sprint(args...))
}
func (cl *consoleLogger) Error(args ...interface{}) {
	cl.output("ERROR", fmt.Sprint(args...))
}
func (cl *consoleLogger) Warn(args ...interface{}) {
	cl.output("WARN", fmt.Sprint(args...))
}
func (cl *consoleLogger) Fatal(args ...interface{}) {
	cl.output("FATAL", fmt.Sprint(args...))
	os.Exit(1)
}

func (cl *consoleLogger) Debugf(format string, args ...interface{}) {
	cl.Debug(fmt.Sprintf(format, args...))
}

func (cl *consoleLogger) Infof(format string, args ...interface{}) {
	cl.Info(fmt.Sprintf(format, args...))
}
func (cl *consoleLogger) Errorf(format string, args ...interface{}) {
	cl.Error(fmt.Sprintf(format, args...))
}
func (cl *consoleLogger) Warnf(format string, args ...interface{}) {
	cl.Warn(fmt.Sprintf(format, args...))
}
func (cl *consoleLogger) Fatalf(format string, args ...interface{}) {
	cl.Fatal(fmt.Sprintf(format, args...))
}

func (cl *consoleLogger) Flush() {

}

func (cl *consoleLogger) SetAdditionalStackDepth(depth int) {
	cl.CallDepth = depth
}

func (cl *consoleLogger) output(level string, message string) {
	cl.mutex.Lock()
	defer cl.mutex.Unlock()
	fmt.Print(time.Now().Format("2006-01-02 15:04:05"))
	fmt.Print(" [" + level + "] ")
	var (
		file string = "???"
		line int    = 0
	)
	if cl.CallDepth <= 0 {
		file, line = getCallerInfo(2)
	} else {
		file, line = getCallerInfo(cl.CallDepth)
	}
	fmt.Printf("%v:%v ", file, line)
	fmt.Println(message)
}
