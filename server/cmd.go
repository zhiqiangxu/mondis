package server

import "github.com/zhiqiangxu/qrpc"

const (
	// SetCmd for set
	SetCmd qrpc.Cmd = iota
	// SetResp is resp for SetCmd
	SetResp
	// GetCmd for get
	GetCmd
	// GetRespCmd is resp for GetCmd
	GetRespCmd
	// DeleteCmd for delete
	DeleteCmd
	// DeleteRespCmd is resp for DeleteCmd
	DeleteRespCmd
	// CommitCmd for commit
	CommitCmd
	// CommitRespCmd is resp for CommitCmd
	CommitRespCmd
	// DiscardCmd for discard, this cmd has no resp
	DiscardCmd
)
