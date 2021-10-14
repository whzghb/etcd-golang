package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	grpcprom "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

type MyClient struct {
	TlsConfig  *tls.Config
}

var (
	dialTimeout = 5 * time.Second
	endpoints = []string{"172.20.42.70:2379"}
)

func (c *MyClient) Client()  {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: endpoints,
		DialOptions: []grpc.DialOption{
			grpc.WithUnaryInterceptor(grpcprom.UnaryClientInterceptor),
			grpc.WithStreamInterceptor(grpcprom.StreamClientInterceptor),
		},
		TLS: c.TlsConfig,
		DialTimeout: dialTimeout,
		//Username:    "root",
		//Password:    "123",
	})
	if err != nil {
		log.Fatal("new", err)
	}
	defer cli.Close()

	// get a key so it shows up in the metrics as a range RPC
	cli.Get(context.TODO(), "test_key")

	// listen for all Prometheus metrics
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}

	//donec := make(chan struct{})
	//go func() {
	//	//defer close(donec)
	//	http.Serve(ln, promhttp.Handler())
	//}()
	//defer func() {
	//	ln.Close()
	//	//<-donec
	//}()

	go http.Serve(ln, promhttp.Handler())

	// make an http request to fetch all Prometheus metrics
	url := "http://" + ln.Addr().String() + "/metrics"
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("fetch error: %v", err)
	}
	b, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		log.Fatalf("fetch error: reading %s: %v", url, err)
	}
	//fmt.Println(string(b))

	// confirm range request in metrics
	for _, l := range strings.Split(string(b), "\n") {
		if strings.Contains(l, `grpc_client_started_total{grpc_method="Range"`) {
			fmt.Println(l)
			break
		}
	}
}
