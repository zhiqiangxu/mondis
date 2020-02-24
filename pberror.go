package kvrpc

import "fmt"

type pbError struct {
	Code int32
	Msg  string
}

func newPBError(code int32, msg string) *pbError {
	return &pbError{Code: code, Msg: msg}
}

func (pbe *pbError) Error() string {
	return fmt.Sprintf("pbError code:%d msg:%s", pbe.Code, pbe.Msg)
}
