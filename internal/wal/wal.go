package wal

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"sync"

	ingestpb "github.com/heyyakash/tickdb/proto/gen/ingest"
)

type WAL struct {
	mu   sync.Mutex
	file *os.File
}

func New(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f}, nil
}

func (w *WAL) Append(record any) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := json.Marshal(record)
	if err != nil {
		return err
	}

	_, err = w.file.Write(append(data, '\n'))

	if err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WAL) Replay() []*ingestpb.Point {
	w.mu.Lock()
	defer w.mu.Unlock()

	f, err := os.Open(w.file.Name())
	if err != nil {
		log.Fatalf("Unable to open file for wal replay")
	}
	defer f.Close()

	points := []*ingestpb.Point{}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		var p ingestpb.Point
		if err := json.Unmarshal([]byte(line), &p); err != nil {
			log.Fatalf("Couldn't unmarhsal line to valid json")
		}
		points = append(points, &p)
	}

	return points
}

func (w *WAL) Close() error {
	return w.file.Close()
}
