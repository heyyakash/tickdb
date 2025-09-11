package memtable

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"

	ingestpb "github.com/heyyakash/tickdb/proto/gen/ingest"
)

type MemTableService struct {
	MemTable map[string][]*ingestpb.Point
	sync.RWMutex
}

type SSTableEntry struct {
	key   string            `json:"key"`
	value []*ingestpb.Point `json:"value"`
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

	if len(m.MemTable) >= 2 {
		m.flushMemtable()
	}
}

func (m *MemTableService) flushMemtable() {
	// get the current working dir
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal("Error in flushing Memtable, couldn't get working directory : ", err)
	}

	sstablePath := filepath.Join(cwd, "sstable", "sstable01.sst")
	// create the sstable file
	f, err := os.Create(sstablePath)
	if err != nil {
		log.Fatal("Error in flushing Memtable, couldn't create sstable", err)
	}
	defer f.Close()

	// sort the keys
	keys := make([]string, 0, len(m.MemTable))
	for k, _ := range m.MemTable {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	index := make(map[string]int64)

	for _, v := range keys {
		offset, _ := f.Seek(0, io.SeekCurrent)
		entry := SSTableEntry{
			key:   v,
			value: m.MemTable[v],
		}
		entryInBytes, err := json.Marshal(entry)
		if err != nil {
			log.Fatal("Error in flushing Memtable, couldn't marshal entry json to bytes", err)
		}
		if err := binary.Write(f, binary.LittleEndian, int32(len(entryInBytes))); err != nil {
			log.Fatal("Error writing length to SSTable :", err)
		}
		_, err = f.Write(entryInBytes)
		if err != nil {
			log.Fatal("Error writing to SSTable : ", err)
		}
		index[v] = offset
	}

	indexOffset, _ := f.Seek(0, io.SeekCurrent)
	indexInBytes, err := json.Marshal(index)
	if err != nil {
		log.Fatal("Error Marshaling index JSON to bytes :", err)
	}
	if err := binary.Write(f, binary.LittleEndian, int32(len(indexInBytes))); err != nil {
		log.Fatal("Error writing length of Index to SSTable :", err)
	}
	_, err = f.Write(indexInBytes)
	if err != nil {
		log.Fatal("Error writing index to SSTable : ", err)
	}

	if err := binary.Write(f, binary.LittleEndian, uint64(indexOffset)); err != nil {
		log.Fatal("Error writing length of Index to SSTable :", err)
	}

}

func (m *MemTableService) LogMemTable() {
	m.RWMutex.Lock()
	defer m.RWMutex.Unlock()

	for k, v := range m.MemTable {
		fmt.Println(k, v)
	}
}
