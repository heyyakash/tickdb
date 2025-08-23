package main

import (
	"log"
	"net"

	"github.com/heyyakash/tickdb/internal/server"
	"github.com/heyyakash/tickdb/internal/wal"
	ingestpb "github.com/heyyakash/tickdb/proto/gen/ingest"
	"google.golang.org/grpc"
)

var port = "50051"

func main() {
	log.Println("Starting TickDB")
	wal, err := wal.New("wal.log")
	if err != nil {
		log.Fatalf("Could't create WAL : %v", err.Error())
	}

	// setup grpc server
	grpc_server := grpc.NewServer()
	ingestpb.RegisterInjestServiceServer(grpc_server, server.NewInjestServer(wal))

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Couldn't start server : %s", err.Error())
	}

	log.Printf("TickDB Listening at port : %s", port)

	if err := grpc_server.Serve(lis); err != nil {
		log.Fatalf("grpc couldn't listen to port %s : %v", port, err.Error())
	}

}
