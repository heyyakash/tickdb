package server

import (
	"net/http"

	"github.com/gin-gonic/gin"
	ingestpipeline "github.com/heyyakash/tickdb/internal/ingest-pipeline"
	ingestpb "github.com/heyyakash/tickdb/proto/gen/ingest"
)

type IngestRestService struct {
	pipelineService *ingestpipeline.PipelineService
}

func NewIngestRestServer(pipelineService *ingestpipeline.PipelineService) *IngestRestService {
	restService := IngestRestService{
		pipelineService: pipelineService,
	}
	return &restService
}

func (i *IngestRestService) SetupHandlers(r *gin.Engine) {
	api := r.Group("ingest")
	api.POST("single", i.handleDataPoint)
	api.POST("batch", i.handleBatchDataPoints)
}

func (i *IngestRestService) handleDataPoint(ctx *gin.Context) {
	var body ingestpb.Point

	if err := ctx.ShouldBindJSON(&body); err != nil {
		ctx.JSON(http.StatusBadRequest, ingestpb.WriteResponse{Accepted: 0, Rejected: 1, Error: "Invalid data scheme"})
		return
	}

	if err := i.pipelineService.AddDataPoint(&body); err != nil {
		ctx.JSON(http.StatusBadRequest, ingestpb.WriteResponse{Accepted: 0, Rejected: 1, Error: err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, ingestpb.WriteResponse{Accepted: 1, Rejected: 0})
}

func (i *IngestRestService) handleBatchDataPoints(ctx *gin.Context) {
	var body []ingestpb.Point
	accepted := 0
	rejected := 0

	if err := ctx.ShouldBindJSON(&body); err != nil {
		ctx.JSON(http.StatusBadRequest, ingestpb.WriteResponse{Accepted: 0, Rejected: 1, Error: "Invalid data scheme"})
		return
	}

	for _, point := range body {
		if err := i.pipelineService.AddDataPoint(&point); err != nil {
			rejected += 1
		} else {
			accepted += 1
		}
	}

	ctx.JSON(http.StatusOK, ingestpb.WriteResponse{Accepted: uint64(accepted), Rejected: uint64(rejected)})
	return
}
