package server

import (
	"github.com/zhiqiangxu/kvrpc/pb"
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
		req  pb.SetRequest
		resp pb.SetResponse
	)

	err := req.Unmarshal(frame.Payload)
	if err != nil {
		resp.Code = CodeInvalidRequest
		resp.Msg = err.Error()
		bytes, _ := resp.Marshal()
		err := writeRespBytes(writer, frame, SetRespCmd, bytes)
		if err != nil {
			logger.Instance().Error("writeRespBytes", zap.Error(err))
		}
		frame.Close()
		return
	}

	switch frame.Flags.IsDone() {
	case true:

		handleSet(cmd.s.kvdb, &req, &resp)

		bytes, _ := resp.Marshal()
		err = writeRespBytes(writer, frame, SetRespCmd, bytes)
		if err != nil {
			logger.Instance().Error("writeRespBytes", zap.Error(err))
		}
	case false:
		txn := cmd.s.kvdb.NewTransaction(true)

		handleTxnSet(txn, &req, &resp)
		{
			bytes, _ := resp.Marshal()
			err = writeStreamRespBytes(writer, frame, SetRespCmd, bytes, false)
			if err != nil {
				logger.Instance().Error("writeStreamRespBytes", zap.Error(err))
				return
			}
		}

		var (
			getReq     pb.GetRequest
			getResp    pb.GetResponse
			deleteReq  pb.DeleteRequest
			deleteResp pb.DeleteResponse
			commitResp pb.CommitResponse
		)
		for {
			nextFrame := <-frame.FrameCh()
			if nextFrame == nil {
				txn.Discard()
				err = writeStreamRespBytes(writer, frame, DiscardRespCmd, nil, true)
				if err != nil {
					logger.Instance().Error("nil writeStreamRespBytes", zap.Error(err))
				}
				return
			}
			switch nextFrame.Cmd {
			case SetCmd:
				handleTxnSet(txn, &req, &resp)
				{
					bytes, _ := resp.Marshal()
					err = writeStreamRespBytes(writer, frame, SetRespCmd, bytes, false)
					if err != nil {
						logger.Instance().Error("SetCmd writeStreamRespBytes", zap.Error(err))
						return
					}
				}
			case GetCmd:
				handleTxnGet(txn, &getReq, &getResp)
				{
					bytes, _ := getResp.Marshal()
					err = writeStreamRespBytes(writer, frame, GetRespCmd, bytes, false)
					if err != nil {
						logger.Instance().Error("GetCmd writeStreamRespBytes", zap.Error(err))
						return
					}
				}
			case DeleteCmd:
				handleTxnDelete(txn, &deleteReq, &deleteResp)
				{
					bytes, _ := deleteResp.Marshal()
					err = writeStreamRespBytes(writer, frame, DeleteRespCmd, bytes, false)
					if err != nil {
						logger.Instance().Error("DeleteCmd writeStreamRespBytes", zap.Error(err))
						return
					}
				}
			case CommitCmd:
				handleTxnCommit(txn, &commitResp)
				{
					bytes, _ := commitResp.Marshal()
					err = writeStreamRespBytes(writer, frame, CommitRespCmd, bytes, false)
					if err != nil {
						logger.Instance().Error("CommitCmd writeStreamRespBytes", zap.Error(err))
						return
					}
				}
			case DiscardCmd:
				txn.Discard()
				err = writeStreamRespBytes(writer, frame, DiscardRespCmd, nil, true)
				if err != nil {
					logger.Instance().Error("DiscardCmd writeStreamRespBytes", zap.Error(err))
				}
				return
			}
		}

	}
}
