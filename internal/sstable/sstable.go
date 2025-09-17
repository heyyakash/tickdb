package sstable

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	memtable "github.com/heyyakash/tickdb/internal/mem-table"
	ingestpb "github.com/heyyakash/tickdb/proto/gen/ingest"
)

type SSTableEntry struct {
	Key   string            `json:"key"`
	Value []*ingestpb.Point `json:"value"`
}

type SSTableService struct {
	m *memtable.MemTableService
}

func NewSSTableService(m *memtable.MemTableService) *SSTableService {
	return &SSTableService{
		m: m,
	}
}

func (s *SSTableService) Flush(walStartTime int64) {
	s.m.Lock()
	defer s.m.Unlock()
	// structure of sstable
	// DATA BLOCK -> [len(bytes)][data json bytes]
	// INDEX BLOCK -> [len(index)][index json bytes]
	// FOOTER -> index offset

	// get the current working dir
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal("Error in flushing Memtable, couldn't get working directory : ", err)
	}
	walStartTimeString := strconv.FormatInt(walStartTime, 10)
	currentTimeStampString := strconv.FormatInt(time.Now().Unix(), 10)
	sstableName := walStartTimeString + "-" + currentTimeStampString + ".sst"
	sstablePath := filepath.Join(cwd, "sstable", sstableName)

	//create dirs if not there
	if err = os.MkdirAll(filepath.Dir(sstablePath), 0755); err != nil {
		log.Fatal("Couldn't create subdirectort for sstable")
	}

	// create the sstable file
	f, err := os.Create(sstablePath)
	if err != nil {
		log.Fatal("Error in flushing Memtable, couldn't create sstable", err)
	}
	defer f.Close()

	// sort the keys
	keys := make([]string, 0, len(s.m.MemTable))
	for k, _ := range s.m.MemTable {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	index := make(map[string]int64)

	for _, v := range keys {
		offset, _ := f.Seek(0, io.SeekCurrent)
		entry := SSTableEntry{
			Key:   v,
			Value: s.m.MemTable[v],
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

	log.Print("Successfully flushed the Memtable")

}
