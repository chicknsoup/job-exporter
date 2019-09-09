package main

import (
	"context"
	"flag"
	"fmt"
	"job-exporter/overseer"
	"log"
	"net/http"
	"net/http/pprof"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	timeout = time.Duration(30000 * time.Millisecond)
)

var (
	addr     string
	endpoint *url.URL
	port     int
	yarns    string
	storms   string
	memlimit uint64
	isdebug  bool
	dummy    bool
)

func prog(state overseer.State) {

	log.Println("yarn-storm-exporter v2.0 by chinhnc")
	flag.IntVar(&port, "port", 9653, "The port to serve the endpoint from.")
	flag.StringVar(&yarns, "yarn.servers", "", "Comma separated list of yarn yarnServers in the format http://host:port")
	flag.StringVar(&storms, "storm.servers", "", "Comma separated list of storm yarnServers in the format http://host:port")
	flag.Uint64Var(&memlimit, "mem.limit", 419430400, "Memory limit in bytes, default: 400MB") //default 400MB
	flag.BoolVar(&isdebug, "debug", false, "Enable/disable debug mode")
	flag.BoolVar(&dummy, "job-exporter-slave", true, "Just a dummy flag")

	flag.Parse()

	client := &http.Client{
		Timeout: timeout,
	}

	c := YarnCollector(client, yarns, storms)
	prometheus.MustRegister(c)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	if isdebug == true {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

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

func main() {
	overseer.Run(overseer.Config{
		Program:   prog,
		Debug:     true,
		NoRestart: false,
	})
}
