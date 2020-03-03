package server

import (
	"github.com/zhiqiangxu/mondis/pb"
	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/util/logger"
	"go.uber.org/zap"
)

// CmdScan for scan
type CmdScan struct {
	s *Server
}

// ServeQRPC implements qrpc.Handler
func (cmd *CmdScan) ServeQRPC(writer qrpc.FrameWriter, frame *qrpc.RequestFrame) {
	var (
		scanReq  pb.ScanRequest
		scanResp pb.ScanResponse
	)

	err := scanReq.Unmarshal(frame.Payload)
	if err != nil {
		scanResp.Code = CodeInvalidRequest
		scanResp.Msg = err.Error()
		bytes, _ := scanResp.Marshal()
		err := writeRespBytes(writer, frame, ScanRespCmd, bytes)
		if err != nil {
			logger.Instance().Error("writeRespBytes", zap.Error(err))
		}
		frame.Close()
		return
	}

	switch frame.Flags.IsDone() {
	case true:

		handleScan(cmd.s.kvdb, &scanReq, &scanResp)

		bytes, _ := scanResp.Marshal()
		err = writeRespBytes(writer, frame, ScanRespCmd, bytes)
		if err != nil {
			logger.Instance().Error("writeRespBytes", zap.Error(err))
		}
	case false:
		txn := cmd.s.kvdb.NewTransaction(frame.Cmd.Opaque() == 1)
		defer txn.Discard()

		handleScan(txn, &scanReq, &scanResp)
		{
			bytes, _ := scanResp.Marshal()
			err = writeStreamRespBytes(writer, frame, ScanRespCmd, bytes, false)
			if err != nil {
				logger.Instance().Error("writeStreamRespBytes", zap.Error(err))
				return
			}
		}

		handleTxnContinuedFrame(writer, frame, txn)

	}
}
