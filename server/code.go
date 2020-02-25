package server

const (
	// CodeOK for ok
	CodeOK int32 = iota
	// CodeInvalidRequest for invalid request
	CodeInvalidRequest
	// CodeInternalError for internal error
	CodeInternalError
	// CodeTxnTooBig for transaction too big
	CodeTxnTooBig
	// CodeKeyNotFound for key not found
	CodeKeyNotFound
)
