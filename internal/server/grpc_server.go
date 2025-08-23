package server

import (
	"context"
	"log"

	ingestpipeline "github.com/heyyakash/tickdb/internal/ingest-pipeline"
	ingestpb "github.com/heyyakash/tickdb/proto/gen/ingest"
)

type IngestService struct {
	ingestpb.UnimplementedInjestServiceServer
	pipelineService *ingestpipeline.PipelineService
}

func NewInjestServer(p *ingestpipeline.PipelineService) *IngestService {
	return &IngestService{pipelineService: p}
}

// Handle Single Data Points
func (i *IngestService) Write(ctx context.Context, req *ingestpb.WriteRequest) (*ingestpb.WriteResponse, error) {
	if req.GetPoint() == nil {
		return &ingestpb.WriteResponse{Rejected: 1, Error: "no measurement provided", Accepted: 0}, nil
	}

	err := i.pipelineService.AddDataPoint(req.GetPoint())
	if err != nil {
		log.Printf("Pipeline error : %v", err)
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
		if err := i.pipelineService.AddDataPoint(point); err != nil {
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
