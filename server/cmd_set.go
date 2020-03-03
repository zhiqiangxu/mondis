package server

import (
	"github.com/zhiqiangxu/mondis/pb"
	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/util/logger"
	"go.uber.org/zap"
)

// CmdSet for set
type CmdSet struct {
	s *Server
}

// ServeQRPC implements qrpc.Handler
func (cmd *CmdSet) ServeQRPC(writer qrpc.FrameWriter, frame *qrpc.RequestFrame) {
	var (
		setReq  pb.SetRequest
		setResp pb.SetResponse
	)

	err := setReq.Unmarshal(frame.Payload)
	if err != nil {
		setResp.Code = CodeInvalidRequest
		setResp.Msg = err.Error()
		bytes, _ := setResp.Marshal()
		err := writeRespBytes(writer, frame, SetRespCmd, bytes)
		if err != nil {
			logger.Instance().Error("writeRespBytes", zap.Error(err))
		}
		frame.Close()
		return
	}

	switch frame.Flags.IsDone() {
	case true:

		handleSet(cmd.s.kvdb, &setReq, &setResp)

		bytes, _ := setResp.Marshal()
		err = writeRespBytes(writer, frame, SetRespCmd, bytes)
		if err != nil {
			logger.Instance().Error("writeRespBytes", zap.Error(err))
		}
	case false:
		txn := cmd.s.kvdb.NewTransaction(true)
		defer txn.Discard()

		handleTxnSet(txn, &setReq, &setResp)
		{
			bytes, _ := setResp.Marshal()
			err = writeStreamRespBytes(writer, frame, SetRespCmd, bytes, false)
			if err != nil {
				logger.Instance().Error("writeStreamRespBytes", zap.Error(err))
				return
			}
		}

		handleTxnContinuedFrame(writer, frame, txn)

	}
}
