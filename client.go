package kvrpc

import (
	"github.com/zhiqiangxu/kvrpc/pb"
	"github.com/zhiqiangxu/kvrpc/server"
	"github.com/zhiqiangxu/qrpc"
)

type (
	// ClientOption for Client
	ClientOption struct {
		QrpcConfig qrpc.ConnectionConfig
	}
	// Client for kvrpc
	Client struct {
		con *qrpc.Connection
	}

	// KVClient is implemneted by Client
	KVClient interface {
		KVOP
		Update(func(t Txn) error) error
		View(func(t Txn) error) error
	}
)

// NewClient is ctor for Client
func NewClient(addr string, option ClientOption) (c KVClient) {
	con := qrpc.NewConnectionWithReconnect([]string{addr}, option.QrpcConfig, nil)
	c = &Client{con: con}
	return
}

func setReq2PB(k, v []byte, meta *VMetaReq) pb.SetRequest {
	req := pb.SetRequest{Key: k, Value: v}
	if meta != nil {
		req.Meta = &pb.VMetaReq{TTL: uint64(meta.TTL), Tag: uint32(meta.Tag)}
	}
	return req
}

func parseSetResp(resp qrpc.Response) (err error) {
	frame, err := resp.GetFrame()
	if err != nil {
		return
	}

	var setResp pb.SetResponse
	err = setResp.Unmarshal(frame.Payload)
	if err != nil {
		return
	}

	if setResp.Code != 0 {
		err = newPBError(setResp.Code, setResp.Msg)
		return
	}

	return
}

// Set for implement KVClient
func (c *Client) Set(k, v []byte, meta *VMetaReq) (err error) {

	req := setReq2PB(k, v, meta)
	bytes, _ := req.Marshal()

	_, resp, err := c.con.Request(server.SetCmd, qrpc.NBFlag, bytes)
	if err != nil {
		return
	}

	err = parseSetResp(resp)

	return
}

func parseGetResp(resp qrpc.Response) (v []byte, meta VMetaResp, err error) {
	frame, err := resp.GetFrame()
	if err != nil {
		return
	}

	var getResp pb.GetResponse
	err = getResp.Unmarshal(frame.Payload)
	if err != nil {
		return
	}

	if getResp.Code != 0 {
		err = newPBError(getResp.Code, getResp.Msg)
		return
	}

	v = getResp.Value
	meta.ExpiresAt = getResp.Meta.ExpiresAt
	meta.Tag = byte(getResp.Meta.Tag)

	return
}

// Get for implement KVClient
func (c *Client) Get(k []byte) (v []byte, meta VMetaResp, err error) {
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

	var deleteResp pb.DeleteResponse
	err = deleteResp.Unmarshal(frame.Payload)
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
func (c *Client) Update(fn func(t Txn) error) (err error) {
	txn := newClientTxn(c, true)
	defer txn.Discard()

	err = fn(txn)
	if err != nil {
		return
	}

	err = txn.Commit()
	return
}

// View for implement KVClient
func (c *Client) View(fn func(t Txn) error) (err error) {
	txn := newClientTxn(c, false)
	defer txn.Discard()

	err = fn(txn)

	return
}
