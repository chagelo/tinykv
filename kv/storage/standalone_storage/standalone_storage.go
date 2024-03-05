package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	badger "github.com/Connor1996/badger"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	db := engine_util.CreateDB(conf.DBPath, true)
	if db == nil {
		return nil
	}

	return &StandAloneStorage{
		db: db,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.db.NewTransaction(false)
	return &cfReader{s: s, txn: txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, modify := range batch {
		data, ok := modify.Data.(storage.Put)
		if ok {
			engine_util.PutCF(s.db, data.Cf, data.Key, data.Value)
		} else {
			newdata, _ := modify.Data.(storage.Delete)
			engine_util.DeleteCF(s.db, newdata.Cf, newdata.Key)
		}
	}

	return nil
}

type cfReader struct {
	s   *StandAloneStorage
	txn *badger.Txn
}

func (cfr *cfReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCF(cfr.s.db, cf, key)
	if err != nil {
		return nil, nil
	}

	return val, nil
}

func (cfr *cfReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, cfr.txn)
}

func (cfr *cfReader) Close() {
	cfr.txn.Discard()
	cfr.s.db.Close()
}
