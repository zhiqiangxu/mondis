package server

import (
	"time"

	"github.com/zhiqiangxu/kvrpc"
	"github.com/zhiqiangxu/kvrpc/pb"
	"github.com/zhiqiangxu/kvrpc/provider"
	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/util/logger"
	"go.uber.org/zap"
)

func handleTxnContinuedFrame(
	writer qrpc.FrameWriter,
	frame *qrpc.RequestFrame,
	txn kvrpc.Txn) {
	var (
		getReq     pb.GetRequest
		getResp    pb.GetResponse
		deleteReq  pb.DeleteRequest
		deleteResp pb.DeleteResponse
		setReq     pb.SetRequest
		setResp    pb.SetResponse
		commitResp pb.CommitResponse
		err        error
		close      bool
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
			close = false
			err = setResp.Unmarshal(nextFrame.Payload)
			if err != nil {
				close = true
				setResp.Code = CodeInvalidRequest
				setResp.Msg = err.Error()
			} else {
				handleTxnSet(txn, &setReq, &setResp)
			}

			{
				bytes, _ := setResp.Marshal()
				err = writeStreamRespBytes(writer, frame, SetRespCmd, bytes, false)
				if err != nil {
					logger.Instance().Error("SetCmd writeStreamRespBytes", zap.Error(err))
					return
				}
			}
			if close {
				frame.Close()
				return
			}
		case GetCmd:
			close = false
			err = getReq.Unmarshal(nextFrame.Payload)
			if err != nil {
				close = true
				getResp.Code = CodeInvalidRequest
				getResp.Msg = err.Error()
			} else {
				handleTxnGet(txn, &getReq, &getResp)
			}

			{
				bytes, _ := getResp.Marshal()
				err = writeStreamRespBytes(writer, frame, GetRespCmd, bytes, false)
				if err != nil {
					logger.Instance().Error("GetCmd writeStreamRespBytes", zap.Error(err))
					return
				}
			}
			if close {
				frame.Close()
				return
			}
		case DeleteCmd:
			close = false
			err = deleteReq.Unmarshal(nextFrame.Payload)
			if err != nil {
				close = true
				deleteResp.Code = CodeInvalidRequest
				deleteResp.Msg = err.Error()
			} else {
				handleTxnDelete(txn, &deleteReq, &deleteResp)
			}

			{
				bytes, _ := deleteResp.Marshal()
				err = writeStreamRespBytes(writer, frame, DeleteRespCmd, bytes, false)
				if err != nil {
					logger.Instance().Error("DeleteCmd writeStreamRespBytes", zap.Error(err))
					return
				}
			}
			if close {
				frame.Close()
				return
			}
		case CommitCmd:
			handleTxnCommit(txn, &commitResp)
			{
				bytes, _ := commitResp.Marshal()
				err = writeStreamRespBytes(writer, frame, CommitRespCmd, bytes, true)
				if err != nil {
					logger.Instance().Error("CommitCmd writeStreamRespBytes", zap.Error(err))
				}
				return
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

func handleTxnSet(txn kvrpc.Txn, req *pb.SetRequest, resp *pb.SetResponse) {
	meta := metaFromSetRequest(req)
	err := txn.Set(req.Key, req.Value, meta)
	if err != nil {

		if err == provider.ErrTxnTooBig {
			resp.Code = CodeTxnTooBig
			resp.Msg = err.Error()
		} else {
			resp.Code = CodeInternalError
			resp.Msg = err.Error()
		}
		return

	}

	resp.Code = CodeOK
	resp.Msg = ""
}

func handleSet(kvdb kvrpc.KVDB, req *pb.SetRequest, resp *pb.SetResponse) {
	meta := metaFromSetRequest(req)
	err := kvdb.Set(req.Key, req.Value, meta)
	if err != nil {

		resp.Code = CodeInternalError
		resp.Msg = err.Error()

		return
	}

	resp.Code = CodeOK
	resp.Msg = ""
}

func handleTxnGet(txn kvrpc.Txn, req *pb.GetRequest, resp *pb.GetResponse) {
	value, meta, err := txn.Get(req.Key)
	if err != nil {
		if err == provider.ErrKeyNotFound {
			resp.Code = CodeKeyNotFound
			resp.Msg = err.Error()
		} else {
			resp.Code = CodeInternalError
			resp.Msg = err.Error()
		}
		return
	}

	resp.Code = CodeOK
	resp.Msg = ""
	resp.Value = value
	resp.Meta = &pb.VMetaResp{ExpiresAt: meta.ExpiresAt, Tag: uint32(meta.Tag)}
}

func handleGet(kvdb kvrpc.KVDB, req *pb.GetRequest, resp *pb.GetResponse) {
	value, meta, err := kvdb.Get(req.Key)
	if err != nil {
		if err == provider.ErrKeyNotFound {
			resp.Code = CodeKeyNotFound
			resp.Msg = err.Error()
		} else {
			resp.Code = CodeInternalError
			resp.Msg = err.Error()
		}

		return
	}

	resp.Code = CodeOK
	resp.Msg = ""
	resp.Value = value
	resp.Meta = &pb.VMetaResp{ExpiresAt: meta.ExpiresAt, Tag: uint32(meta.Tag)}
}

func handleDelete(kvdb kvrpc.KVDB, req *pb.DeleteRequest, resp *pb.DeleteResponse) {
	err := kvdb.Delete(req.Key)
	if err != nil {
		resp.Code = CodeInternalError
		resp.Msg = err.Error()
		return
	}

	resp.Code = CodeOK
	resp.Msg = ""
}

func handleTxnDelete(txn kvrpc.Txn, req *pb.DeleteRequest, resp *pb.DeleteResponse) {
	err := txn.Delete(req.Key)
	if err != nil {
		if err == provider.ErrTxnTooBig {
			resp.Code = CodeTxnTooBig
			resp.Msg = err.Error()
		} else {
			resp.Code = CodeInternalError
			resp.Msg = err.Error()
		}
		return
	}

	resp.Code = CodeOK
	resp.Msg = ""
}

func handleTxnCommit(txn kvrpc.Txn, resp *pb.CommitResponse) {
	err := txn.Commit()
	if err != nil {
		resp.Code = CodeInternalError
		resp.Msg = err.Error()
		return
	}

	resp.Code = CodeOK
	resp.Msg = ""
}
func metaFromSetRequest(req *pb.SetRequest) *kvrpc.VMetaReq {
	if req.Meta == nil {
		return nil
	}

	return &kvrpc.VMetaReq{TTL: time.Duration(req.Meta.TTL), Tag: byte(req.Meta.Tag)}
}

func writeStreamRespBytes(writer qrpc.FrameWriter, frame *qrpc.RequestFrame, respCmd qrpc.Cmd, bytes []byte, end bool) (err error) {
	flag := qrpc.StreamFlag
	if end {
		flag |= qrpc.StreamEndFlag
	}
	writer.StartWrite(frame.RequestID, respCmd, flag)
	writer.WriteBytes(bytes)

	err = writer.EndWrite()

	return
}

func writeRespBytes(writer qrpc.FrameWriter, frame *qrpc.RequestFrame, respCmd qrpc.Cmd, bytes []byte) (err error) {
	writer.StartWrite(frame.RequestID, respCmd, 0)
	writer.WriteBytes(bytes)

	err = writer.EndWrite()

	return
}
