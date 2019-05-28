package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	addr     string
	endpoint *url.URL
	port         int
	yarns      string
	storms string
)


func init() {
	log.Println("yarn-storm-exporter v1.0 by chinhnc")
	flag.IntVar(&port, "port", 9653, "The port to serve the endpoint from.")
	flag.StringVar(&yarns, "yarn.servers", "", "Comma separated list of yarn yarnServers in the format http://host:port")
	flag.StringVar(&storms, "storm.servers", "", "Comma separated list of storm yarnServers in the format http://host:port")
	flag.Parse()
}

func main() {
	c := YarnCollector(yarns, storms)
	prometheus.MustRegister(c)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%v", port),
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	go func() {
		_ = <-sigs
		log.Println("main: received SIGINT or SIGTERM, shutting down")
		context, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := srv.Shutdown(context); err != nil {
			log.Printf("main: failed to shutdown endpoint with err=%#v\n", err)
		}
	}()

	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Printf("main: failure while serving endpoint, err=%#v\n", err)
	}
}