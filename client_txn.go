package kvrpc

import (
	"errors"

	"github.com/zhiqiangxu/kvrpc/pb"
	"github.com/zhiqiangxu/kvrpc/server"
	"github.com/zhiqiangxu/qrpc"
)

// ClientTxn for client side transaction
type ClientTxn struct {
	c                 *Client
	update            bool
	commitOrDiscarded bool
	sw                qrpc.StreamWriter
	resp              qrpc.Response
}

var _ Txn = (*ClientTxn)(nil)

func newClientTxn(c *Client, update bool) *ClientTxn {
	return &ClientTxn{c: c, update: update}
}

// Set for implement Txn
func (txn *ClientTxn) Set(k, v []byte, meta *VMetaReq) (err error) {
	if !txn.update {
		err = ErrMutateForROTxn
		return
	}

	req := setReq2PB(k, v, meta)
	bytes, _ := req.Marshal()

	_, err = txn.request(server.SetCmd, bytes, false)
	if err != nil {
		return
	}

	err = parseSetResp(txn.resp)
	if err != nil {
		return
	}

	return
}

func (txn *ClientTxn) request(cmd qrpc.Cmd, bytes []byte, end bool) (noop bool, err error) {
	if txn.sw != nil {
		txn.sw.StartWrite(cmd)
		txn.sw.WriteBytes(bytes)
		err = txn.sw.EndWrite(end)
		return
	}

	if cmd == server.CommitCmd || cmd == server.DiscardCmd {
		// noop if transaction empty
		noop = true
		return
	}

	flag := qrpc.StreamFlag
	if end {
		flag |= qrpc.StreamEndFlag
	}
	sw, resp, err := txn.c.con.StreamRequest(cmd, flag, bytes)
	if err != nil {
		return
	}

	txn.sw = sw
	txn.resp = resp
	return
}

// Get for implement Txn
func (txn *ClientTxn) Get(k []byte) (v []byte, meta VMetaResp, err error) {
	req := pb.GetRequest{Key: k}
	bytes, _ := req.Marshal()

	_, err = txn.request(server.GetCmd, bytes, false)
	if err != nil {
		return
	}

	v, meta, err = parseGetResp(txn.resp)

	return
}

// ErrMutateForROTxn when trying to delete/set on readonly txn
var ErrMutateForROTxn = errors.New("mutate for readonly txn")

// Delete for implement Txn
func (txn *ClientTxn) Delete(k []byte) (err error) {
	if !txn.update {
		err = ErrMutateForROTxn
		return
	}

	req := pb.DeleteRequest{Key: k}
	bytes, _ := req.Marshal()

	_, err = txn.request(server.DeleteCmd, bytes, false)
	if err != nil {
		return
	}

	err = parseDeleteResp(txn.resp)

	return
}

func parseCommitResp(resp qrpc.Response) (err error) {
	frame, err := resp.GetFrame()
	if err != nil {
		return
	}

	var commitResp pb.CommitResponse
	err = commitResp.Unmarshal(frame.Payload)
	if err != nil {
		return
	}

	if commitResp.Code != 0 {
		err = newPBError(commitResp.Code, commitResp.Msg)
		return
	}

	return
}

// Commit for implement Txn
func (txn *ClientTxn) Commit() (err error) {
	noop, err := txn.request(server.CommitCmd, nil, true)
	if err != nil {
		return
	}

	if noop {
		return
	}

	err = parseCommitResp(txn.resp)

	return
}

// Discard for implement Txn
func (txn *ClientTxn) Discard() {
	noop, err := txn.request(server.DiscardCmd, nil, true)
	if err != nil {
		return
	}

	if noop {
		return
	}

	_, err = txn.resp.GetFrame()

	return
}
