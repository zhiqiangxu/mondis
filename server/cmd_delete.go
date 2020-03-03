package server

import (
	"github.com/zhiqiangxu/mondis/pb"
	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/util/logger"
	"go.uber.org/zap"
)

// CmdDelete for delete
type CmdDelete struct {
	s *Server
}

// ServeQRPC implements qrpc.Handler
func (cmd *CmdDelete) ServeQRPC(writer qrpc.FrameWriter, frame *qrpc.RequestFrame) {
	var (
		deleteReq  pb.DeleteRequest
		deleteResp pb.DeleteResponse
	)

	err := deleteReq.Unmarshal(frame.Payload)
	if err != nil {
		deleteResp.Code = CodeInvalidRequest
		deleteResp.Msg = err.Error()
		bytes, _ := deleteResp.Marshal()
		err := writeRespBytes(writer, frame, DeleteRespCmd, bytes)
		if err != nil {
			logger.Instance().Error("writeRespBytes", zap.Error(err))
		}
		frame.Close()
		return
	}

	switch frame.Flags.IsDone() {
	case true:

		handleDelete(cmd.s.kvdb, &deleteReq, &deleteResp)

		bytes, _ := deleteResp.Marshal()
		err = writeRespBytes(writer, frame, DeleteRespCmd, bytes)
		if err != nil {
			logger.Instance().Error("writeRespBytes", zap.Error(err))
		}
	case false:
		txn := cmd.s.kvdb.NewTransaction(true)
		defer txn.Discard()

		handleTxnDelete(txn, &deleteReq, &deleteResp)
		{
			bytes, _ := deleteResp.Marshal()
			err = writeStreamRespBytes(writer, frame, DeleteRespCmd, bytes, false)
			if err != nil {
				logger.Instance().Error("writeStreamRespBytes", zap.Error(err))
				return
			}
		}

		handleTxnContinuedFrame(writer, frame, txn)

	}
}
