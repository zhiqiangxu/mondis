package client

import (
	"errors"

	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/pb"
	"github.com/zhiqiangxu/mondis/server"
	"github.com/zhiqiangxu/qrpc"
)

// Txn for client side transaction
type Txn struct {
	c          *Client
	update     bool
	sw         qrpc.StreamWriter
	resp       qrpc.Response
	firstFrame *qrpc.Frame
}

var _ mondis.Txn = (*Txn)(nil)

func newTxn(c *Client, update bool) *Txn {
	return &Txn{c: c, update: update}
}

// Set for implement mondis.Txn
func (txn *Txn) Set(k, v []byte, meta *mondis.VMetaReq) (err error) {
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

	respFrame, err := txn.getRespFrame()
	if err != nil {
		return
	}

	err = parseSetRespFromFrame(respFrame)

	return
}

func (txn *Txn) getRespFrame() (respFrame *qrpc.Frame, err error) {
	if txn.firstFrame != nil {
		respFrame = <-txn.firstFrame.FrameCh()
		return
	}

	respFrame, err = txn.resp.GetFrame()
	if err == nil {
		txn.firstFrame = respFrame
	}
	return
}

func (txn *Txn) request(cmd qrpc.Cmd, bytes []byte, end bool) (noop bool, err error) {
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

	if txn.update {
		cmd = qrpc.CmdWithOpaque(cmd, 1)
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

// Exists for implement mondis.Client
func (txn *Txn) Exists(k []byte) (exists bool, err error) {
	req := pb.ExistsRequest{Key: k}
	bytes, _ := req.Marshal()

	_, err = txn.request(server.ExistsCmd, bytes, false)
	if err != nil {
		return
	}

	respFrame, err := txn.getRespFrame()
	if err != nil {
		return
	}

	exists, err = parseExistsRespFromFrame(respFrame)
	return
}

// Get for implement mondis.Txn
func (txn *Txn) Get(k []byte) (v []byte, meta mondis.VMetaResp, err error) {
	req := pb.GetRequest{Key: k}
	bytes, _ := req.Marshal()

	_, err = txn.request(server.GetCmd, bytes, false)
	if err != nil {
		return
	}

	respFrame, err := txn.getRespFrame()
	if err != nil {
		return
	}

	v, meta, err = parseGetRespFromFrame(respFrame)

	return
}

// ErrMutateForROTxn when trying to delete/set on readonly txn
var ErrMutateForROTxn = errors.New("mutate for readonly txn")

// Delete for implement mondis.Txn
func (txn *Txn) Delete(k []byte) (err error) {
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

	respFrame, err := txn.getRespFrame()
	if err != nil {
		return
	}

	err = parseDeleteRespFromFrame(respFrame)

	return
}

func parseCommitResp(respFrame *qrpc.Frame) (err error) {

	var commitResp pb.CommitResponse
	err = commitResp.Unmarshal(respFrame.Payload)
	if err != nil {
		return
	}

	if commitResp.Code != 0 {
		err = newPBError(commitResp.Code, commitResp.Msg)
		return
	}

	return
}

// Commit for implement mondis.Txn
func (txn *Txn) Commit() (err error) {
	noop, err := txn.request(server.CommitCmd, nil, true)
	if err != nil {
		return
	}

	if noop {
		return
	}

	respFrame, err := txn.getRespFrame()
	if err != nil {
		return
	}

	err = parseCommitResp(respFrame)

	return
}

// Discard for implement mondis.Txn
func (txn *Txn) Discard() {
	noop, err := txn.request(server.DiscardCmd, nil, true)
	if err != nil {
		return
	}

	if noop {
		return
	}

	_, err = txn.getRespFrame()

	return
}

// Scan for implement mondis.Txn
func (txn *Txn) Scan(option mondis.ScanOption) (entries []mondis.Entry, err error) {
	if option.Limit <= 0 {
		return
	}

	if option.Limit > mondis.MaxEntry {
		option.Limit = mondis.MaxEntry
	}

	bytes := scanOption2Bytes(option)

	_, err = txn.request(server.ScanCmd, bytes, false)
	if err != nil {
		return
	}

	respFrame, err := txn.getRespFrame()
	if err != nil {
		return
	}

	entries, err = parseScanRespFromFrame(respFrame)

	return
}
