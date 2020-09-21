package main

import (
	"context"
	"fmt"
	"github.com/xidongc/qproxy"
	"github.com/xidongc/qproxy/config"
	"github.com/xidongc/qproxy/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
	"time"
)

func main() {
	// TODO: implement
	//for _, f := range os.Args {
	//	switch f {
	//		case "version", "v", "--version", "-version", "-v":
	//			qproxy.PrintVersions()
	//			os.Exit(0)
	//		}
	//	}
	conf := config.ParseConfig()
	if conf.Profile != "" {
		f, err := os.Create(conf.Profile)
		if err != nil {
			log.Fatal(err)
		}
		err = pprof.StartCPUProfile(f)
		if err != nil {
			log.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}

	server, err := qproxy.NewServer(conf)
	if err != nil {
		log.Fatal(err)
	}

	// Created shared context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// GRPC server setup
	grpcServer := grpc.NewServer()
	rpc.RegisterQProxyServer(grpcServer, server)
	reflection.Register(grpcServer)
	go func() {
		addr := fmt.Sprintf(":%d", conf.GRPCPort)
		l, err := net.Listen("tcp4", addr)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		if err := grpcServer.Serve(l); err != nil {
			// Because we graceful stop, just log this out
			// GracefulStop will kill l, but we should not
			// throw an error to let it shut down gracefully
			log.Printf("failed to serve: %v", err)
		}
		cancel()
	}()

	// HTTP grpc-gateway setup
	mux := http.NewServeMux()
	if err := qproxy.AddRoutes(mux, server); err != nil {
		log.Fatalf("adding routes: %v", err)
	}
	httpServer := &http.Server{
		Handler:      mux,
		WriteTimeout: conf.WriteTimeout,
		ReadTimeout:  conf.ReadTimeout,
		IdleTimeout:  conf.IdleTimeout,
	}
	go func() {
		addr := fmt.Sprintf(":%d", conf.HTTPPort)
		l, err := net.Listen("tcp4", addr)
		if err != nil {
			log.Fatalf("failed to create http listen addr: %v", err)
		}
		if err := httpServer.Serve(l); err != nil {
			log.Printf("failed to serve: %v", err)
		}
		cancel()
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)

	log.Printf("listening on for grpc:%v", conf.GRPCPort)
	log.Printf("listening on for http:%v", conf.HTTPPort)

	select {
	case <-sigs:
	case <-ctx.Done():
	}

	log.Printf("Got shutdown signal")

	// Useful for graceful shutdowns, or taking nodes out of rotation
	// before shutting the service down
	if conf.TermSleep > 0 {
		log.Printf("sleeping for %+v before running shutdown", conf.TermSleep)
		time.Sleep(conf.TermSleep)
	}

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("shutdown: %v", err)
	}
	grpcServer.GracefulStop()

	if conf.MemProfile != "" {
		f, err := os.Create(conf.MemProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}
}
