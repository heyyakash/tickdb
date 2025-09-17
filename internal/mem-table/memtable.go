package memtable

import (
	"log"
	"sync"

	ingestpb "github.com/heyyakash/tickdb/proto/gen/ingest"
)

type MemTableService struct {
	MemTable   map[string][]*ingestpb.Point
	PointCount uint16
	sync.RWMutex
}

func NewMemTableService(memtable map[string][]*ingestpb.Point) *MemTableService {
	return &MemTableService{
		MemTable: memtable,
	}
}

func (m *MemTableService) FlushMemTable() {
	m.RWMutex.Lock()
	defer m.RWMutex.Unlock()

	m.MemTable = make(map[string][]*ingestpb.Point)
	m.PointCount = 0
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
	// log.Print("New Memtable\n")
	// m.LogMemTable()
	m.PointCount += 1
}

func (m *MemTableService) LogMemTable() {
	m.RWMutex.Lock()
	defer m.RWMutex.Unlock()

	for k, v := range m.MemTable {
		log.Println(k, v)
	}
}

func (m *MemTableService) CountPoints() uint16 {
	return m.PointCount
}
