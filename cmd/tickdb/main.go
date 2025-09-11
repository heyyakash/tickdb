package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	ingestpipeline "github.com/heyyakash/tickdb/internal/ingest-pipeline"
	memtable "github.com/heyyakash/tickdb/internal/mem-table"
	"github.com/heyyakash/tickdb/internal/server"
	"github.com/heyyakash/tickdb/internal/wal"
	ingestpb "github.com/heyyakash/tickdb/proto/gen/ingest"
	"google.golang.org/grpc"
)

var port = "50051"

func initWAL() *wal.WAL {
	wal, err := wal.New("wal.log")
	if err != nil {
		log.Fatalf("Could't create WAL : %v", err.Error())
	}
	return wal
}

func initalizeMemTable() *memtable.MemTableService {
	MemTable := make(map[string][]*ingestpb.Point)
	MemTableService := memtable.NewMemTableService(MemTable)
	return MemTableService
}

func initPipelineService(wal *wal.WAL, MemTableService *memtable.MemTableService) *ingestpipeline.PipelineService {
	pipelineService := ingestpipeline.NewPipeline(wal, MemTableService)
	pipelineService.WALReplay()
	return pipelineService
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	cwd, _ := os.Getwd()
	log.Println("Starting TickDB from : ", cwd)

	//setup wal
	wal := initWAL()

	//setup memTable service
	memtableService := initalizeMemTable()

	//setup pipeline service
	pipelineService := initPipelineService(wal, memtableService)

	// setup grpc server
	grpc_server := grpc.NewServer()
	ingestpb.RegisterInjestServiceServer(grpc_server, server.NewInjestServer(pipelineService))

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Couldn't start server : %s", err.Error())
	}

	// start grpc server go-routine
	go func() {
		log.Printf("TickDB grpc_server is listening at port : %s", port)
		if err := grpc_server.Serve(lis); err != nil {
			log.Fatalf("grpc couldn't listen to port %s : %v", port, err.Error())
		}
	}()

	// setup rest server
	r := gin.Default()
	ingestRestService := server.NewIngestRestServer(pipelineService)

	// register rest handlers for ingesting data
	ingestRestService.SetupHandlers(r)

	httpServer := &http.Server{
		Addr:    ":8020",
		Handler: r.Handler(),
	}

	//start httpServer goroutine
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Could'nt start rest api server : %v", err.Error())
		}
	}()

	<-ctx.Done()
	log.Println("Signal to Shutdown received! Shutting down gracefully...")

	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	//Stopping rest api server
	log.Println("Stopping rest api server...")
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Error in stopping the rest api server : %v", err.Error())
	}

	//Stopping grpc server
	log.Println("Stopping grpc server...")
	grpc_server.GracefulStop()

	//Stopping ingest channel
	pipelineService.Close()
	log.Println("TickDB Stopped gracefully")

}
