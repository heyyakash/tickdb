package wal

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	ingestpb "github.com/heyyakash/tickdb/proto/gen/ingest"
)

type WAL struct {
	mu                sync.Mutex
	path              string
	walStartTimeStamp int64
	file              *os.File
}

func New(path string) (*WAL, error) {
	cwd, _ := os.Getwd()
	walPath := filepath.Join(cwd, "wal")

	if err := os.MkdirAll(walPath, 0755); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(walPath)
	if err != nil {
		return nil, err
	}

	for _, v := range entries {
		if !v.IsDir() && strings.HasSuffix(v.Name(), "new.log") {
			f, err := os.OpenFile(filepath.Join(walPath, v.Name()), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return nil, err
			}
			walStartTSString := strings.Split(v.Name(), ".")[0]
			walStartTS, err := strconv.ParseInt(walStartTSString, 10, 64)
			if err != nil {
				log.Fatal("Couldn't extract Wal Start Timestamp :", err)
			}
			return &WAL{file: f, path: walPath, walStartTimeStamp: walStartTS}, nil
		}
	}

	currentTimeStamp := time.Now().Unix()
	newWalName := strconv.FormatInt(currentTimeStamp, 10)
	f, err := os.Create(filepath.Join(walPath, newWalName+".new.log"))
	if err != nil {
		return nil, err
	}
	return &WAL{file: f, path: walPath, walStartTimeStamp: currentTimeStamp}, nil
}

func (w *WAL) GetWalStartTime() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.walStartTimeStamp
}

// func New(path string) (*WAL, error) {
// 	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &WAL{file: f}, nil
// }

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

func (w *WAL) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()

	fileName := w.file.Name()
	newFileName := strings.ReplaceAll(fileName, "new.log", "closed.log")
	// newFilePath := filepath.Join(w.path, newFileName)
	if err := os.Rename(fileName, newFileName); err != nil {
		log.Fatal("Error renaming Wal file :", err)
	}
	w.file.Close()
	currentTimeStamp := time.Now().Unix()
	newWalName := strconv.FormatInt(currentTimeStamp, 10)
	f, err := os.Create(filepath.Join(w.path, newWalName+".new.log"))
	if err != nil {
		log.Fatal("Couln't create new WAL")
	}
	w.file = f

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
