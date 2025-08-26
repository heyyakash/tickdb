package ingestpipeline

import (
	"context"
	"errors"
	"log"

	memtable "github.com/heyyakash/tickdb/internal/mem-table"
	"github.com/heyyakash/tickdb/internal/wal"
	ingestpb "github.com/heyyakash/tickdb/proto/gen/ingest"
)

type PipelineService struct {
	wal              *wal.WAL
	memtableSerivice *memtable.MemTableService
	Pipeline         chan *ingestpb.Point
	ctx              context.Context
	cancel           context.CancelFunc
}

func NewPipeline(w *wal.WAL, m *memtable.MemTableService) *PipelineService {
	ctx, cancel := context.WithCancel(context.Background())
	p := &PipelineService{
		wal:              w,
		memtableSerivice: m,
		Pipeline:         make(chan *ingestpb.Point, 100),
		ctx:              ctx,
		cancel:           cancel,
	}

	go p.ProcessDataPoint()
	return p
}

func (p *PipelineService) ProcessDataPoint() {
	for {
		select {
		case point := <-p.Pipeline:
			if err := p.wal.Append(point); err != nil {
				log.Printf("Couldn't process datapoint")
			}
			p.memtableSerivice.AddToMemTable(point)
			p.memtableSerivice.LogMemTable()
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *PipelineService) AddDataPoint(point *ingestpb.Point) error {
	select {
	case p.Pipeline <- point:
		return nil
	case <-p.ctx.Done():
		return context.Canceled
	default:
		return errors.New("Pipeline chanel is full")
	}
}

func (p *PipelineService) Close() {
	p.cancel()
}
