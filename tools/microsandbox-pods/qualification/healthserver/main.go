// This application is built only by the native qualification fixture.
package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type server struct {
	healthpb.UnimplementedHealthServer
}

func (server) Check(_ context.Context, request *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	if request.Service != "ready" {
		return nil, status.Error(codes.NotFound, "unknown service")
	}
	state := healthpb.HealthCheckResponse_NOT_SERVING
	if _, err := os.Stat("/tmp/grpc-ready"); err == nil {
		state = healthpb.HealthCheckResponse_SERVING
	}
	return &healthpb.HealthCheckResponse{Status: state}, nil
}

func main() {
	listener, err := net.Listen("tcp4", "0.0.0.0:9090")
	if err != nil {
		panic(err)
	}
	grpcServer := grpc.NewServer()
	healthpb.RegisterHealthServer(grpcServer, server{})
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			panic(err)
		}
	}()
	if err := http.ListenAndServe("127.0.0.1:8123", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "loopback-only-application")
	})); err != nil {
		panic(err)
	}
}
