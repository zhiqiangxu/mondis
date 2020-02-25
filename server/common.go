package server

import (
	"time"

	"github.com/zhiqiangxu/kvrpc"
	"github.com/zhiqiangxu/kvrpc/pb"
	"github.com/zhiqiangxu/kvrpc/provider"
	"github.com/zhiqiangxu/qrpc"
)

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
		resp.Code = CodeInternalError
		resp.Msg = err.Error()
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
		resp.Code = CodeInternalError
		resp.Msg = err.Error()
		return
	}

	resp.Code = CodeOK
	resp.Msg = ""
	resp.Value = value
	resp.Meta = &pb.VMetaResp{ExpiresAt: meta.ExpiresAt, Tag: uint32(meta.Tag)}
}

func handleDelete(kvdb kvrpc.KVDB, req *pb.SetRequest, resp *pb.SetResponse) {
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
