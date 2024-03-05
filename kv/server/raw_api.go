package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	response := &kvrpcpb.RawGetResponse{}

	value, _ := reader.GetCF(req.GetCf(), req.GetKey())
	if value == nil {
		response.NotFound = true
	}
	response.Value = value

	return response, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	modify := append([]storage.Modify{},
		storage.Modify{
			Data: storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			}})
	err := server.storage.Write(nil, modify)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	modify := append([]storage.Modify{},
		storage.Modify{
			Data: storage.Delete{
				Key: req.Key,
				Cf:  req.Cf,
			}})

	response := &kvrpcpb.RawDeleteResponse{}
	err := server.storage.Write(nil, modify)
	if err != nil {
		return response, err
	}

	return response, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	kvs := []*kvrpcpb.KvPair{}
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	iter := reader.IterCF(req.GetCf())
	idx := 0
	for iter.Seek(req.StartKey); iter.Valid() && idx < int(req.Limit); iter.Next() {
		key := iter.Item().Key()
		value, _ := iter.Item().Value()
		kvs = append(kvs, &kvrpcpb.KvPair{Key: key, Value: value})
		idx++
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}
