package mpsrc

import (
	"errors"
)

const (
	VERSION           = "1.0.2"
)

var (
	ErrLogFileNotFound error = errors.New("not found lofile in flag")
	ErrNilRedisConn    error = errors.New("get redispool conn error")
	ErrRegister        error = errors.New("Registe watcher failed")
	ErrInvalidKey      error = errors.New("the key is invalid")
	ErrValueStore      error = errors.New("storevalue failed")
	ErrDeleteVaule     error = errors.New("delete value errors")
	ErrStoreKey        error = errors.New("store value errors")
	ErrAllMysqlDown    error = errors.New("all mysql instance are dead")
	ErrSetCache        error = errors.New("set cache errors")
	ErrInvalidVersion  error = errors.New("version is invalid")
	ErrInfoType        error = errors.New("type must be fans or likes")
	ErrOpt             error = errors.New("opt must be add or delete")
)
