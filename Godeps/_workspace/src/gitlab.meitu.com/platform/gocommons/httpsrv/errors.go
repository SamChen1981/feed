package httpsrv

import (
	"fmt"
)

type HeaderMissingError struct {
	Header string
}

func (hm HeaderMissingError) Error() string {
	return fmt.Sprintf("header %s is required", hm.Header)
}

type HeaderInvalidError struct {
	Header string
}

func (hi HeaderInvalidError) Error() string {
	return fmt.Sprintf("header %s is invalid", hi.Header)
}

type ParamMissingError struct {
	Param string
}

func (pm ParamMissingError) Error() string {
	return fmt.Sprintf("param %s is required", pm.Param)
}

type ParamInvalidError struct {
	Param string
}

func (pi ParamInvalidError) Error() string {
	return fmt.Sprintf("param %s is invalid", pi.Param)
}

type InternalServerError struct {
	Desc string
}

func (is InternalServerError) Error() string {
	return fmt.Sprintf("Internal Server Error: %v", is.Desc)
}

type APIError struct {
	Code    int // 建议用1001这样的4位整数表示, 前两位10表示模块,　后两位01表示错误码
	Message string
}

func (ae APIError) Error() string {
	return ae.Message
}
