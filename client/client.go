package client

import (
	"github.com/zhiqiangxu/kvrpc"
	"github.com/zhiqiangxu/kvrpc/pb"
	"github.com/zhiqiangxu/kvrpc/provider"
	"github.com/zhiqiangxu/kvrpc/server"
	"github.com/zhiqiangxu/qrpc"
)

type (
	// Option for Client
	Option struct {
		QrpcConfig qrpc.ConnectionConfig
	}
	// Client for kvrpc
	Client struct {
		con *qrpc.Connection
	}

	// KVClient is implemneted by Client
	KVClient interface {
		kvrpc.KVOP
		Update(func(t kvrpc.Txn) error) error
		View(func(t kvrpc.Txn) error) error
	}
)

// New is ctor for Client
func New(addr string, option Option) (c KVClient) {
	con := qrpc.NewConnectionWithReconnect([]string{addr}, option.QrpcConfig, nil)
	c = &Client{con: con}
	return
}

func setReq2PB(k, v []byte, meta *kvrpc.VMetaReq) pb.SetRequest {
	req := pb.SetRequest{Key: k, Value: v}
	if meta != nil {
		req.Meta = &pb.VMetaReq{TTL: int64(meta.TTL), Tag: uint32(meta.Tag)}
	}
	return req
}

func parseSetResp(resp qrpc.Response) (err error) {
	frame, err := resp.GetFrame()
	if err != nil {
		return
	}

	return parseSetRespFromFrame(frame)

}

func parseSetRespFromFrame(respFrame *qrpc.Frame) (err error) {
	var setResp pb.SetResponse
	err = setResp.Unmarshal(respFrame.Payload)
	if err != nil {
		return
	}

	if setResp.Code != 0 {

		if setResp.Code == server.CodeTxnTooBig {
			err = provider.ErrTxnTooBig
		} else {
			err = newPBError(setResp.Code, setResp.Msg)
		}

		return
	}

	return
}

// Set for implement KVClient
func (c *Client) Set(k, v []byte, meta *kvrpc.VMetaReq) (err error) {

	req := setReq2PB(k, v, meta)
	bytes, _ := req.Marshal()

	_, resp, err := c.con.Request(server.SetCmd, qrpc.NBFlag, bytes)
	if err != nil {
		return
	}

	err = parseSetResp(resp)

	return
}

func parseGetResp(resp qrpc.Response) (v []byte, meta kvrpc.VMetaResp, err error) {
	frame, err := resp.GetFrame()
	if err != nil {
		return
	}

	v, meta, err = parseGetRespFromFrame(frame)
	return
}

func parseGetRespFromFrame(respFrame *qrpc.Frame) (v []byte, meta kvrpc.VMetaResp, err error) {
	var getResp pb.GetResponse
	err = getResp.Unmarshal(respFrame.Payload)
	if err != nil {
		return
	}

	if getResp.Code != 0 {

		if getResp.Code == server.CodeKeyNotFound {
			err = provider.ErrKeyNotFound
		} else {
			err = newPBError(getResp.Code, getResp.Msg)
		}

		return
	}

	v = getResp.Value
	meta.ExpiresAt = getResp.Meta.ExpiresAt
	meta.Tag = byte(getResp.Meta.Tag)

	return
}

// Get for implement KVClient
func (c *Client) Get(k []byte) (v []byte, meta kvrpc.VMetaResp, err error) {
	req := pb.GetRequest{Key: k}
	bytes, _ := req.Marshal()

	_, resp, err := c.con.Request(server.GetCmd, qrpc.NBFlag, bytes)
	if err != nil {
		return
	}

	v, meta, err = parseGetResp(resp)

	return
}

func parseDeleteResp(resp qrpc.Response) (err error) {
	frame, err := resp.GetFrame()
	if err != nil {
		return
	}

	err = parseDeleteRespFromFrame(frame)
	return
}

func parseDeleteRespFromFrame(respFrame *qrpc.Frame) (err error) {
	var deleteResp pb.DeleteResponse
	err = deleteResp.Unmarshal(respFrame.Payload)
	if err != nil {
		return
	}

	if deleteResp.Code != 0 {
		err = newPBError(deleteResp.Code, deleteResp.Msg)
		return
	}

	return
}

// Delete for implement KVClient
func (c *Client) Delete(k []byte) (err error) {
	req := pb.DeleteRequest{Key: k}
	bytes, _ := req.Marshal()

	_, resp, err := c.con.Request(server.DeleteCmd, qrpc.NBFlag, bytes)
	if err != nil {
		return
	}

	err = parseDeleteResp(resp)

	return
}

// Update for implement KVClient
func (c *Client) Update(fn func(t kvrpc.Txn) error) (err error) {
	txn := newTxn(c, true)
	defer txn.Discard()

	err = fn(txn)
	if err != nil {
		return
	}

	err = txn.Commit()
	return
}

// View for implement KVClient
func (c *Client) View(fn func(t kvrpc.Txn) error) (err error) {
	txn := newTxn(c, false)
	defer txn.Discard()

	err = fn(txn)

	return
}
