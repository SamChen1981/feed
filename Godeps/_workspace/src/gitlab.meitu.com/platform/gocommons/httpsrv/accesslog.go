package httpsrv

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/gin-gonic/gin"
	commomsio "gitlab.meitu.com/platform/gocommons/io"
)

// AccessLogPattern Options:
//  %a - Remote IP address
//  %b - Bytes sent, excluding HTTP headers, or '-' if no bytes were sent
//  %B - Bytes sent, excluding HTTP headers
//  %H - Request protocol
//  %m - Request method
//  %q - Query string (prepended with a '?' if it exists, otherwise an empty string
//  %r - First line of the request
//  %s - HTTP status code of the response
//  %t - Time the request was received, in the format [18/Sep/2011:19:18:28 -0400].
//  %U - Requested URL path
//  %D - Time taken to process the request, in millis
//  %T - Time taken to process the request, in seconds
//  %{xxx}i - Incoming request headers
//  %{xxx}o - Outgoing response headers
type AccessLogPattern string

const (
	// CommonLogFormat is the Common Log Format (CLF).
	DefaultLogPattern = "meitu.com %a %D - %t \"%r\" %s %b"
	defaultPrefix     = "access_log."
	defaultSuffix     = ""
	defaultDateFormat = ".2006-01-02"
	defaultDirectory  = "./logs"
)

type AccessLogItem struct {
	URL            *url.URL
	RequestHeader  http.Header
	ResponseHeader http.Header
	RemoteIP       string
	Method         string
	Proto          string
	ReceivedAt     time.Time
	Latency        time.Duration
	BytesSent      int
	StatusCode     int
}

func (item *AccessLogItem) Reset() {
	item = &AccessLogItem{}
}

func (a AccessLogItem) QueryString() string {
	if a.URL.RawQuery != "" {
		return "?" + a.URL.RawQuery
	}

	return ""
}

type AccessLogger struct {
	options      AccessLogOptions
	textTemplate *template.Template
	mutex        sync.Mutex // ensures atomic writes; protects the following fields
	logFile      *os.File
	lastChecked  int64
	dateStamp    string
}

type AccessLogOptions struct {
	Pattern        AccessLogPattern
	Prefix         string
	Suffix         string
	Directory      string
	Rotatable      bool
	FileDateFormat string
}

func (alo AccessLogOptions) GetFileDateFormat() string {
	if alo.FileDateFormat == "" {
		return defaultDateFormat
	}

	return alo.FileDateFormat
}

func (alo AccessLogOptions) GetPrefix() string {
	if alo.Prefix == "" {
		return defaultPrefix
	}

	return alo.Prefix
}
func (alo AccessLogOptions) GetSuffix() string {
	if alo.Suffix == "" {
		return defaultSuffix
	}

	return alo.Suffix
}
func (alo AccessLogOptions) GetPattern() string {
	if alo.Pattern == "" {
		return DefaultLogPattern
	}

	return string(alo.Pattern)
}

func (alo AccessLogOptions) GetDirectory() string {
	if alo.Directory == "" {
		return defaultDirectory
	}

	return alo.Directory
}

func NewAccessLogger(options AccessLogOptions) *AccessLogger {
	_, valid := commomsio.EnsureDirectory(options.Directory)
	if !valid {
		panic("invalid access log directory!")
	}
	accessLogger := &AccessLogger{
		options: options,
		mutex:   sync.Mutex{},
	}
	accessLogger.initTemplate()
	accessLogger.dateStamp = time.Now().Format(options.GetFileDateFormat())
	accessLogger.openLogFile()
	return accessLogger
}

var logPatternAdapter = strings.NewReplacer(
	"%a", "{{.RemoteIP}}",
	"%b", "{{.BytesSent | dashIfLe0}}",
	"%B", "{{.BytesSent}}",
	"%H", "{{.Proto}}",
	"%m", "{{.Method}}",
	"%q", "{{.QueryString}}",
	"%r", "{{.Method}} {{.URL.RequestURI}} {{.Proto}}",
	"%s", "{{.StatusCode}}",
	"%t", "{{if .ReceivedAt}}{{.ReceivedAt.Format \"[02/Jan/2006:15:04:05 -0700]\"}}{{end}}",
	"%U", "{{.URL.Path}}",
	"%D", "{{.Latency | milliseconds}}",
	"%T", "{{if .Latency}}{{.Latency.Seconds | printf \"%.3f\"}}{{end}}",
)

var newLine = byte('\n')

func (a *AccessLogger) openLogFile() {
	var logFilePath string
	if a.options.Rotatable {
		logFilePath = path.Join(a.options.GetDirectory(), a.options.GetPrefix()+a.dateStamp+a.options.GetSuffix())
	} else {
		logFilePath = path.Join(a.options.GetDirectory(), a.options.GetPrefix()+a.options.GetSuffix())
	}
	var err error
	a.logFile, err = os.OpenFile(logFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
}

func (a *AccessLogger) closeLogFile() {
	err := a.logFile.Close()
	if err != nil {
		fmt.Printf("close access log file %v error: %v", a.logFile.Name(), err)
	}
}

func (a *AccessLogger) initTemplate() {

	templateText := logPatternAdapter.Replace(a.options.GetPattern())
	requestHeaderPattern := regexp.MustCompile("%{(.*?)}i")
	templateText = requestHeaderPattern.ReplaceAllString(templateText, "{{range $$i, $$e := index .RequestHeader \"$1\"}}{{if $$i}},{{$$e}}{{else}}{{$$e}}{{end}}{{else}}-{{end}}")
	responseHeaderPattern := regexp.MustCompile("%{(.*?)}o")
	templateText = responseHeaderPattern.ReplaceAllString(templateText, "{{range $$i, $$e := index .ResponseHeader \"$1\"}}{{if $$i}},{{$$e}}{{else}}{{$$e}}{{end}}{{else}}-{{end}}")

	funcMap := template.FuncMap{
		"dashIfEmptyStr": func(value string) string {
			if value == "" {
				return "-"
			}
			return value
		},
		"dashIfLe0": func(value int) string {
			if value <= 0 {
				return "-"
			}
			return fmt.Sprintf("%d", value)
		},
		"microseconds": func(dur time.Duration) string {
			return fmt.Sprintf("%d", dur.Nanoseconds()/1000)
		},
		"milliseconds": func(dur time.Duration) string {
			return fmt.Sprintf("%d", dur.Nanoseconds()/1000000)
		},
	}

	var err error
	a.textTemplate, err = template.New("accessLog").Funcs(funcMap).Parse(templateText)
	if err != nil {
		panic(err)
	}
}

func (a *AccessLogger) render(ali *AccessLogItem) []byte {
	buf := bytes.NewBufferString("")
	err := a.textTemplate.Execute(buf, ali)
	if err != nil {
		panic(err)
	}
	buf.WriteByte(newLine)
	return buf.Bytes()
}

// Log Execute the text template with the data derived from the request, and return a string.
func (a *AccessLogger) Log(ali *AccessLogItem) error {
	if a.options.Rotatable {
		now := time.Now()
		if t := now.Unix(); t-a.lastChecked > 1 { // 每秒钟检查一次
			a.lastChecked = t
			tsDate := now.Format(a.options.GetFileDateFormat())
			if !strings.EqualFold(tsDate, a.dateStamp) {
				a.mutex.Lock()
				defer a.mutex.Unlock()
				if !strings.EqualFold(tsDate, a.dateStamp) {
					a.closeLogFile()
					a.dateStamp = tsDate
					a.openLogFile()
				}
			}
		}
	}

	_, err := a.logFile.Write(a.render(ali))
	return err
}

var logItemPool = sync.Pool{
	New: func() interface{} {
		return &AccessLogItem{}
	},
}

// AccessLoggerFunc 用于记录 http access log
// 与Java Web应用的AccessLog格式一致：DateFormat=".yyyyMMdd-HH"
// pattern="meitu.com %a %D - %t &quot;%r&quot; %s %b"
func AccessLoggerFunc(accessLogger *AccessLogger) gin.HandlerFunc {
	return func(c *gin.Context) {
		logItem := logItemPool.Get().(*AccessLogItem)
		defer logItemPool.Put(logItem)
		logItem.Reset()

		logItem.URL = c.Request.URL
		logItem.RequestHeader = c.Request.Header
		logItem.RemoteIP = getRemoteIP(c.Request)
		logItem.Method = c.Request.Method
		logItem.Proto = c.Request.Proto
		logItem.ReceivedAt = time.Now()

		// Process request
		c.Next()

		logItem.ResponseHeader = c.Writer.Header()
		logItem.BytesSent = c.Writer.Size()
		logItem.StatusCode = c.Writer.Status()
		logItem.Latency = time.Since(logItem.ReceivedAt)
		accessLogger.Log(logItem)
	}
}

func getRemoteIP(r *http.Request) string {
	if remoteIP := strings.TrimSpace(r.Header.Get("X-Real-Ip")); len(remoteIP) > 0 {
		return remoteIP
	} else if remoteIP = strings.TrimSpace(r.Header.Get("X-Forwarded-For")); len(remoteIP) > 0 {
		return strings.Split(remoteIP, ",")[0]
	} else if remoteIP = strings.TrimSpace(r.RemoteAddr); len(remoteIP) > 0 {
		return strings.SplitN(remoteIP, ":", 2)[0]
	}
	return ""
}
