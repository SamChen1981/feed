package httpsrv

import (
	"net/http"

	"github.com/dropbox/godropbox/errors"
	"github.com/gin-gonic/gin"
	"gitlab.meitu.com/platform/gocommons/log"
)

type HandleRequestFunc func(*gin.Context) (interface{}, error)

func MakeHandler(fn HandleRequestFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		resp, err := fn(c)
		if err != nil {
			HandleErrorResponse(c, err)
			return
		}
		HandleOKResponse(c, resp)
	}
}

func RecoveryLoggerFunc(logger log.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				logger.Errorf("RequestUri: %v", c.Request.RequestURI, errors.Wrap(err.(error), "Panic In HTTP Invoke"))
				c.AbortWithStatus(500)
			}
		}()
		c.Next()
	}
}

// 异常响应，json字段命名与php部分一致
type errorResponse struct {
	ErrorCode  int    `json:"errcode"`
	ErrorInner string `json:"error_inner"`
	Message    string `json:"msg"`
	Request    string `json:"request"`
}

type okResponse struct {
	ErrorCode int         `json:"errcode"`
	Data      interface{} `json:"data,omitempty"`
}

func HandleErrorResponse(c *gin.Context, err error) {
	status := http.StatusBadRequest
	errCode := http.StatusBadRequest * 100
	errInner := err.Error()
	message := err.Error()
	switch err.(type) {
	case ParamInvalidError, ParamMissingError, HeaderInvalidError, HeaderMissingError:
		message = "参数错误,请参考接口文档"
	case APIError:
		if ae := err.(APIError); ae.Code != 0 {
			if ae.Code > 10000 {
				errCode = ae.Code
			} else {
				errCode += ae.Code
			}
		}
		message = "API接口错误"
	default:
		errCode = http.StatusInternalServerError * 100
		status = http.StatusInternalServerError
		message = "服务器内部错误"
	}

	er := &errorResponse{
		ErrorCode:  errCode,
		ErrorInner: errInner,
		Message:    message,
		Request:    c.Request.RequestURI,
	}

	c.JSON(status, er)
}

func HandleOKResponse(c *gin.Context, resp interface{}) {
	okResp := &okResponse{
		ErrorCode: 0,
		Data:      resp,
	}
	c.JSON(http.StatusOK, okResp)
}
