package server

import (
	"github.com/zhiqiangxu/mondis/pb"
	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/util/logger"
	"go.uber.org/zap"
)

// CmdGet for get
type CmdGet struct {
	s *Server
}

// ServeQRPC implements qrpc.Handler
func (cmd *CmdGet) ServeQRPC(writer qrpc.FrameWriter, frame *qrpc.RequestFrame) {
	var (
		getReq  pb.GetRequest
		getResp pb.GetResponse
	)

	err := getReq.Unmarshal(frame.Payload)
	if err != nil {
		getResp.Code = CodeInvalidRequest
		getResp.Msg = err.Error()
		bytes, _ := getResp.Marshal()
		err := writeRespBytes(writer, frame, GetRespCmd, bytes)
		if err != nil {
			logger.Instance().Error("writeRespBytes", zap.Error(err))
		}
		frame.Close()
		return
	}

	switch frame.Flags.IsDone() {
	case true:

		handleGet(cmd.s.kvdb, &getReq, &getResp)

		bytes, _ := getResp.Marshal()
		err = writeRespBytes(writer, frame, GetRespCmd, bytes)
		if err != nil {
			logger.Instance().Error("writeRespBytes", zap.Error(err))
		}
	case false:
		txn := cmd.s.kvdb.NewTransaction(frame.Cmd.Opaque() == 1)
		defer txn.Discard()

		handleGet(txn, &getReq, &getResp)
		{
			bytes, _ := getResp.Marshal()
			err = writeStreamRespBytes(writer, frame, GetRespCmd, bytes, false)
			if err != nil {
				logger.Instance().Error("writeStreamRespBytes", zap.Error(err))
				return
			}
		}

		handleTxnContinuedFrame(writer, frame, txn)

	}
}
