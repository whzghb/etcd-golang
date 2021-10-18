package main

import (
	"crypto/tls"

	"fmt"
	"log"
	"time"

	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/pkg/transport"
)

var (
	dialTimeout = 5 * time.Second
	endpoints = []string{"172.20.42.70:2379"}
	tlsConfig *tls.Config
)

type Lock struct {
	Cli   *clientv3.Client
}

func main() {
	cli := getCli()
	// 开启认证
	//cli.AuthEnable(context.TODO())
	defer cli.Close() // make sure to close the client
	lockTest(cli)
	time.Sleep(20*time.Second)
}

func lockTest(cli *clientv3.Client)  {
	l := Lock{Cli:cli}
	//l.LockExample()
	l.LockKey(1, 50)
}

func (l *Lock)LockExample()  {
	fmt.Println("-------------------- lock s1")
	s1, err := concurrency.NewSession(l.Cli)
	if err != nil {
		log.Fatal(err)
	}
	defer s1.Close()

	m1 := concurrency.NewMutex(s1, "lock1")

	// acquire lock for s1， 先让s1获取到锁
	if err := m1.Lock(context.TODO()); err != nil {
		log.Fatal(err)
	}
	fmt.Println("-------- acquired lock for s1")
}


func (l *Lock)LockKey(id int, ttl int64)  {
	now := time.Now().Unix()
	fmt.Println(now)
	lease, err  := l.Cli.Grant(context.Background(), ttl)
	if err != nil{
		log.Fatal(err)
	}
	s, err := concurrency.NewSession(l.Cli, concurrency.WithLease(lease.ID))

	if err != nil {
		log.Fatal(err)
	}
	//不能用close, close将不会等待租期到期即会被其他程序获取到锁
	//defer s.Close()
	//Orphan会在租期到期时释放锁
	defer s.Orphan()
	m := concurrency.NewMutex(s, "leaseLock2")
	// acquire lock for s1
	fmt.Println("WAIT LOCK", id)
	if err := m.Lock(context.Background()); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("acquired lock for s%d\n", id)
	end := time.Now().Unix()
	fmt.Printf("wait time %d\n", end-now)

}

func getCli() *clientv3.Client {
	var err error
	tlsInfo := transport.TLSInfo{
		CertFile:      "tls/kube-etcd-172-20-42-70.pem",
		KeyFile:       "tls/kube-etcd-172-20-42-70-key.pem",
		TrustedCAFile: "tls/kube-ca.pem",
	}
	tlsConfig, err = tlsInfo.ClientConfig()
	if err != nil {
		log.Fatal(err)
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
		TLS:         tlsConfig,
		Username:    "root",
		Password:    "123",
	})
	if err != nil {
		fmt.Println(err)
	}
	return cli
}