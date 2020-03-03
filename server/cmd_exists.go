package server

import (
	"github.com/zhiqiangxu/mondis/pb"
	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/util/logger"
	"go.uber.org/zap"
)

// CmdExists for exists
type CmdExists struct {
	s *Server
}

// ServeQRPC implements qrpc.Handler
func (cmd *CmdExists) ServeQRPC(writer qrpc.FrameWriter, frame *qrpc.RequestFrame) {
	var (
		existsReq  pb.ExistsRequest
		existsResp pb.ExistsResponse
	)

	err := existsReq.Unmarshal(frame.Payload)
	if err != nil {
		existsResp.Code = CodeInvalidRequest
		existsResp.Msg = err.Error()
		bytes, _ := existsResp.Marshal()
		err := writeRespBytes(writer, frame, ExistsRespCmd, bytes)
		if err != nil {
			logger.Instance().Error("writeRespBytes", zap.Error(err))
		}
		frame.Close()
		return
	}

	switch frame.Flags.IsDone() {
	case true:

		handleExists(cmd.s.kvdb, &existsReq, &existsResp)

		bytes, _ := existsResp.Marshal()
		err = writeRespBytes(writer, frame, ExistsRespCmd, bytes)
		if err != nil {
			logger.Instance().Error("writeRespBytes", zap.Error(err))
		}
	case false:
		txn := cmd.s.kvdb.NewTransaction(frame.Cmd.Opaque() == 1)
		defer txn.Discard()

		handleExists(txn, &existsReq, &existsResp)
		{
			bytes, _ := existsResp.Marshal()
			err = writeStreamRespBytes(writer, frame, ExistsRespCmd, bytes, false)
			if err != nil {
				logger.Instance().Error("writeStreamRespBytes", zap.Error(err))
				return
			}
		}

		handleTxnContinuedFrame(writer, frame, txn)

	}
}
