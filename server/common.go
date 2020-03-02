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
	txn kvrpc.ProviderTxn) {
	var (
		getReq     pb.GetRequest
		getResp    pb.GetResponse
		deleteReq  pb.DeleteRequest
		deleteResp pb.DeleteResponse
		setReq     pb.SetRequest
		setResp    pb.SetResponse
		existsReq  pb.ExistsRequest
		existsResp pb.ExistsResponse
		scanReq    pb.ScanRequest
		scanResp   pb.ScanResponse
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
		case ExistsCmd:
			close = false
			err = existsReq.Unmarshal(nextFrame.Payload)
			if err != nil {
				close = true
				existsResp.Code = CodeInvalidRequest
				existsResp.Msg = err.Error()
			} else {
				handleExists(txn, &existsReq, &existsResp)
			}

			{
				bytes, _ := existsResp.Marshal()
				err = writeStreamRespBytes(writer, frame, ExistsRespCmd, bytes, false)
				if err != nil {
					logger.Instance().Error("ExistsCmd writeStreamRespBytes", zap.Error(err))
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
				handleGet(txn, &getReq, &getResp)
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
		case ScanCmd:
			close = false
			err = scanReq.Unmarshal(nextFrame.Payload)
			if err != nil {
				close = true
				scanResp.Code = CodeInvalidRequest
				scanResp.Msg = err.Error()
			} else {
				handleScan(txn, &scanReq, &scanResp)
			}

			{
				bytes, _ := scanResp.Marshal()
				err = writeStreamRespBytes(writer, frame, ScanRespCmd, bytes, false)
				if err != nil {
					logger.Instance().Error("ScanCmd writeStreamRespBytes", zap.Error(err))
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

func handleTxnSet(txn kvrpc.ProviderTxn, req *pb.SetRequest, resp *pb.SetResponse) {
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

func handleExists(kvop kvrpc.ProviderKVOP, req *pb.ExistsRequest, resp *pb.ExistsResponse) {
	exists, err := kvop.Exists(req.Key)
	if err != nil {

		resp.Code = CodeInternalError
		resp.Msg = err.Error()

		return
	}

	resp.Code = CodeOK
	resp.Msg = ""
	resp.Exists = exists
}

func handleGet(kvop kvrpc.ProviderKVOP, req *pb.GetRequest, resp *pb.GetResponse) {
	value, meta, err := kvop.Get(req.Key)
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

func handleTxnDelete(txn kvrpc.ProviderTxn, req *pb.DeleteRequest, resp *pb.DeleteResponse) {
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

func copyBytes(in []byte) (out []byte) {
	out = make([]byte, len(in))
	copy(out, in)
	return
}

func handleScan(kvop kvrpc.ProviderKVOP, req *pb.ScanRequest, resp *pb.ScanResponse) {
	pso := req.ProviderScanOption
	option := kvrpc.ProviderScanOption{Reverse: pso.Reverse, Prefix: pso.Prefix, Offset: pso.Offset}
	limit := int(req.Limit)
	if limit == 0 {
		goto DONE
	}
	if limit > kvrpc.MaxEntry {
		limit = kvrpc.MaxEntry
	}

	{
		err := kvop.Scan(option, func(key, value []byte, meta kvrpc.VMetaResp) bool {
			keyCopy := copyBytes(key)
			valueCopy := copyBytes(value)
			pbMeta := &pb.VMetaResp{ExpiresAt: meta.ExpiresAt, Tag: uint32(meta.Tag)}
			resp.Entries = append(resp.Entries, &pb.Entry{Key: keyCopy, Value: valueCopy, Meta: pbMeta})

			if len(resp.Entries) >= limit {
				return false
			}
			return true
		})

		if err != nil {
			resp.Code = CodeInternalError
			resp.Msg = err.Error()
			return
		}
	}

DONE:
	resp.Code = CodeOK
	resp.Msg = ""
}

func handleTxnCommit(txn kvrpc.ProviderTxn, resp *pb.CommitResponse) {
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
