package server

import (
	"github.com/zhiqiangxu/kvrpc"
	"github.com/zhiqiangxu/qrpc"
)

type (
	// Option for Server
	Option struct {
	}
	// Server for kvrpc
	Server struct {
		option   Option
		kvoption kvrpc.KVOption
		kvdb     kvrpc.KVDB
		qserver  *qrpc.Server
	}
	// KVServer is implemneted by Server
	KVServer interface {
		Start() error
		Stop() error
	}
)

// New is ctor for Server
func New(addr string, kvdb kvrpc.KVDB, option Option, kvoption kvrpc.KVOption) KVServer {
	s := &Server{option: option, kvoption: kvoption, kvdb: kvdb}

	mux := qrpc.NewServeMux()
	mux.Handle(SetCmd, &CmdSet{s})
	mux.Handle(GetCmd, &CmdGet{s})
	mux.Handle(DeleteCmd, &CmdDelete{s})
	mux.Handle(ScanCmd, &CmdScan{s})
	bindings := []qrpc.ServerBinding{qrpc.ServerBinding{Addr: addr, Handler: mux}}
	qserver := qrpc.NewServer(bindings)

	s.qserver = qserver
	return s
}

// Start server
func (s *Server) Start() (err error) {
	err = s.kvdb.Open(s.kvoption)
	if err != nil {
		return
	}
	return s.qserver.ListenAndServe()
}

// Stop server
func (s *Server) Stop() (err error) {

	err = s.kvdb.Close()
	if err != nil {
		return
	}

	err = s.qserver.Shutdown()
	return
}
