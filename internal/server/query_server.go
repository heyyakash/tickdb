package server

import (
	"log"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	memtable "github.com/heyyakash/tickdb/internal/mem-table"
	"github.com/heyyakash/tickdb/internal/sstable"
	ingestpb "github.com/heyyakash/tickdb/proto/gen/ingest"
)

type QueryServer struct {
	m *memtable.MemTableService
	s *sstable.SSTableService
}

type QueryRequest struct {
	Key                   string `json:"key"`
	FromUnixTimeStampNano string `json:"from_unix_timestamp_nano"`
	ToUnixTimeStampNano   string `json:"to_unix_timestamp_nano"`
}

type QueryResponse struct {
	Success bool              `json:"success"`
	Error   string            `json:"error"`
	Points  []*ingestpb.Point `json:"points"`
}

func NewQueryServer(m *memtable.MemTableService, s *sstable.SSTableService) *QueryServer {
	return &QueryServer{
		m: m,
		s: s,
	}
}

func (q *QueryServer) SetupHandlers(r *gin.Engine) {
	api := r.Group("query")
	api.POST("/", q.HandleQuery)
}

func (q *QueryServer) HandleQuery(ctx *gin.Context) {
	var body QueryRequest
	if err := ctx.BindJSON(&body); err != nil {
		log.Printf("Couldn't extract query body : %v", err)
		ctx.AbortWithStatusJSON(http.StatusBadRequest, QueryResponse{Success: false, Error: "Invalid Request Body"})
		return
	}

	var Points []*ingestpb.Point

	dataPoints, ok := q.m.MemTable[body.Key]
	if !ok {
		//todo implement SSTTable lookup
		ctx.JSON(http.StatusNotFound, QueryResponse{Success: true, Points: Points})
		return
	}

	startTimeStamp, err := strconv.ParseInt(body.FromUnixTimeStampNano, 10, 64)
	if err != nil {
		ctx.AbortWithStatusJSON(http.StatusBadRequest, QueryResponse{Success: false, Error: "Invalid from_unix_timestamp_nano value"})
		return
	}

	endTimeStamp, err := strconv.ParseInt(body.ToUnixTimeStampNano, 10, 64)
	if err != nil {
		ctx.AbortWithStatusJSON(http.StatusBadRequest, QueryResponse{Success: false, Error: "Invalid to_unix_timestamp_nano value"})
		return
	}

	for _, point := range dataPoints {
		if point.TimestampUnixNano >= startTimeStamp && point.TimestampUnixNano <= endTimeStamp {
			Points = append(Points, point)
		}
	}

	ctx.JSON(http.StatusOK, QueryResponse{Success: true, Points: Points})

}
