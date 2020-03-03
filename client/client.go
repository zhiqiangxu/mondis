package client

import (
	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/pb"
	"github.com/zhiqiangxu/mondis/provider"
	"github.com/zhiqiangxu/mondis/server"
	"github.com/zhiqiangxu/qrpc"
)

type (
	// Option for Client
	Option struct {
		QrpcConfig qrpc.ConnectionConfig
	}
	// Client implements mondis.Client
	Client struct {
		con *qrpc.Connection
	}
)

// New is ctor for Client
func New(addr string, option Option) (c mondis.Client) {
	con := qrpc.NewConnectionWithReconnect([]string{addr}, option.QrpcConfig, nil)
	c = &Client{con: con}
	return
}

func setReq2PB(k, v []byte, meta *mondis.VMetaReq) pb.SetRequest {
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

// Set for implement mondis.Client
func (c *Client) Set(k, v []byte, meta *mondis.VMetaReq) (err error) {

	req := setReq2PB(k, v, meta)
	bytes, _ := req.Marshal()

	_, resp, err := c.con.Request(server.SetCmd, qrpc.NBFlag, bytes)
	if err != nil {
		return
	}

	err = parseSetResp(resp)

	return
}

func parseExistsResp(resp qrpc.Response) (exists bool, err error) {
	frame, err := resp.GetFrame()
	if err != nil {
		return
	}

	exists, err = parseExistsRespFromFrame(frame)
	return
}

func parseExistsRespFromFrame(respFrame *qrpc.Frame) (exists bool, err error) {
	var existsResp pb.ExistsResponse
	err = existsResp.Unmarshal(respFrame.Payload)
	if err != nil {
		return
	}

	if existsResp.Code != 0 {

		err = newPBError(existsResp.Code, existsResp.Msg)

		return
	}

	exists = existsResp.Exists

	return
}

// Exists for implement mondis.Client
func (c *Client) Exists(k []byte) (exists bool, err error) {
	req := pb.ExistsRequest{Key: k}
	bytes, _ := req.Marshal()

	_, resp, err := c.con.Request(server.ExistsCmd, qrpc.NBFlag, bytes)
	if err != nil {
		return
	}

	exists, err = parseExistsResp(resp)

	return
}

func parseGetResp(resp qrpc.Response) (v []byte, meta mondis.VMetaResp, err error) {
	frame, err := resp.GetFrame()
	if err != nil {
		return
	}

	v, meta, err = parseGetRespFromFrame(frame)
	return
}

func parseGetRespFromFrame(respFrame *qrpc.Frame) (v []byte, meta mondis.VMetaResp, err error) {
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

// Get for implement mondis.Client
func (c *Client) Get(k []byte) (v []byte, meta mondis.VMetaResp, err error) {
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

// Delete for implement mondis.Client
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

// Update for implement mondis.Client
func (c *Client) Update(fn func(t mondis.Txn) error) (err error) {
	txn := newTxn(c, true)
	defer txn.Discard()

	err = fn(txn)
	if err != nil {
		return
	}

	err = txn.Commit()
	return
}

// View for implement mondis.Client
func (c *Client) View(fn func(t mondis.Txn) error) (err error) {
	txn := newTxn(c, false)
	defer txn.Discard()

	err = fn(txn)

	return
}

func parseScanRespFromFrame(respFrame *qrpc.Frame) (entries []mondis.Entry, err error) {
	var scanResp pb.ScanResponse
	err = scanResp.Unmarshal(respFrame.Payload)
	if err != nil {
		return
	}

	if scanResp.Code != 0 {
		err = newPBError(scanResp.Code, scanResp.Msg)
		return
	}

	entries = make([]mondis.Entry, len(scanResp.Entries))
	for i, entry := range scanResp.Entries {
		meta := mondis.VMetaResp{ExpiresAt: entry.Meta.ExpiresAt, Tag: byte(entry.Meta.Tag)}
		entries[i] = mondis.Entry{Key: entry.Key, Value: entry.Value, Meta: meta}
		entry.Key = nil
		entry.Value = nil
	}

	return
}

func parseScanResp(resp qrpc.Response) (entries []mondis.Entry, err error) {
	frame, err := resp.GetFrame()
	if err != nil {
		return
	}

	entries, err = parseScanRespFromFrame(frame)
	return
}

func scanOption2Bytes(option mondis.ScanOption) (bytes []byte) {
	pso := &pb.ProviderScanOption{Reverse: option.Reverse, Prefix: option.Prefix, Offset: option.Offset}
	req := pb.ScanRequest{ProviderScanOption: pso, Limit: int32(option.Limit)}
	bytes, _ = req.Marshal()
	return
}

// Scan for implement mondis.Client
func (c *Client) Scan(option mondis.ScanOption) (entries []mondis.Entry, err error) {
	if option.Limit <= 0 {
		return
	}

	if option.Limit > mondis.MaxEntry {
		option.Limit = mondis.MaxEntry
	}

	bytes := scanOption2Bytes(option)

	_, resp, err := c.con.Request(server.ScanCmd, qrpc.NBFlag, bytes)
	if err != nil {
		return
	}

	entries, err = parseScanResp(resp)

	return
}
