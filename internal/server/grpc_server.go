package server

import (
	"context"
	"log"

	wal "github.com/heyyakash/tickdb/internal/wal"
	ingestpb "github.com/heyyakash/tickdb/proto/gen/ingest"
)

type IngestService struct {
	ingestpb.UnimplementedInjestServiceServer
	wal *wal.WAL
}

func NewInjestServer(w *wal.WAL) *IngestService {
	return &IngestService{wal: w}
}

// Handle Single Data Points
func (i *IngestService) Write(ctx context.Context, req *ingestpb.WriteRequest) (*ingestpb.WriteResponse, error) {
	if req.GetPoint() == nil {
		return &ingestpb.WriteResponse{Rejected: 1, Error: "no measurement provided", Accepted: 0}, nil
	}

	err := i.wal.Append(req.GetPoint())
	if err != nil {
		log.Printf("WAL append error : %v", err)
		return &ingestpb.WriteResponse{Rejected: 1, Error: err.Error(), Accepted: 0}, err
	}

	return &ingestpb.WriteResponse{Accepted: 1, Rejected: 0}, nil
}

// Handles batched data points
func (i *IngestService) BatchWrite(ctx context.Context, req *ingestpb.BatchWriteRequest) (*ingestpb.WriteResponse, error) {
	var accepted, rejected uint64

	for _, point := range req.GetPoints() {
		if point == nil {
			rejected += 1
		}
		if err := i.wal.Append(point); err != nil {
			rejected += 1
		} else {
			accepted += 1
		}
	}

	return &ingestpb.WriteResponse{
		Accepted: accepted,
		Rejected: rejected,
	}, nil
}
