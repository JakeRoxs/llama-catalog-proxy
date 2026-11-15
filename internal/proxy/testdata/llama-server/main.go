package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

type event struct {
	Event   string `json:"event"`
	PID     int    `json:"pid"`
	Context int    `json:"context"`
}

func main() {
	host := flag.String("host", "127.0.0.1", "listen host")
	port := flag.Int("port", 0, "listen port")
	contextSize := flag.Int("c", 0, "test context")
	flag.Parse()
	identity := event{PID: os.Getpid(), Context: *contextSize}
	writeEvent := func(name string) {
		entry := identity
		entry.Event = name
		data, err := json.Marshal(entry)
		if err != nil {
			log.Fatal(err)
		}
		file, err := os.OpenFile(os.Getenv("CATALOG_PROXY_RELOAD_TEST_EVENTS"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			log.Fatal(err)
		}
		if _, err := file.Write(append(data, '\n')); err != nil {
			log.Fatal(err)
		}
		if err := file.Close(); err != nil {
			log.Fatal(err)
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if _, err := fmt.Fprint(w, `{"status":"ok"}`); err != nil {
			log.Printf("write health response: %v", err)
		}
	})
	mux.HandleFunc("/identity", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(identity); err != nil {
			log.Printf("write identity response: %v", err)
		}
	})
	listener, err := net.Listen("tcp", net.JoinHostPort(*host, strconv.Itoa(*port)))
	if err != nil {
		log.Fatal(err)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)
	server := &http.Server{Handler: mux}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	writeEvent("started")
	select {
	case <-signals:
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err = server.Shutdown(ctx)
		cancel()
		if err != nil {
			log.Fatal(err)
		}
		if err := <-done; err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
		writeEvent("stopped")
	case err := <-done:
		if err != nil {
			log.Fatal(err)
		}
	}
}
