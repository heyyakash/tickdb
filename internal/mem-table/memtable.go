package memtable

import (
	"fmt"
	"sync"

	ingestpb "github.com/heyyakash/tickdb/proto/gen/ingest"
)

type MemTableService struct {
	MemTable map[string][]*ingestpb.Point
	sync.RWMutex
}

func NewMemTableService(memtable map[string][]*ingestpb.Point) *MemTableService {
	return &MemTableService{
		MemTable: memtable,
	}
}

func (m *MemTableService) AddToMemTable(point *ingestpb.Point) {
	m.RWMutex.Lock()
	defer m.RWMutex.Unlock()

	tagString := ""
	for k, v := range point.Tag {
		tagString += "|" + k + "=" + v
	}
	key := point.Measurement + tagString
	m.MemTable[key] = append(m.MemTable[key], point)
}

func (m *MemTableService) LogMemTable() {
	m.RWMutex.Lock()
	defer m.RWMutex.Unlock()

	for k, v := range m.MemTable {
		fmt.Println(k, v)
	}
}
