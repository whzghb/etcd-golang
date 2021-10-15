package main

import (
	"crypto/tls"
	"etcd/election"
	"etcd/lease"
	"etcd/lock"
	"etcd/transfer"
	"etcd/watch"
	"fmt"
	"log"
	"time"

	"etcd/auth"
	"etcd/kv"
	"etcd/client"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/transport"
)

var (
	dialTimeout = 5 * time.Second
	endpoints = []string{"172.20.42.70:2379"}
	tlsConfig *tls.Config
)

func main() {
	cli := getCli()
	// 开启认证
	//cli.AuthEnable(context.TODO())
	defer cli.Close() // make sure to close the client
	//kvTest(cli)
	//authTest(cli)
	//cliTest()
	//leaseTest(cli)
	//watchTest(cli)
	//electionTest(cli)
	//transferTest(cli)
	lockTest(cli)
}

func kvTest(cli *clientv3.Client)  {
	k := &kv.KV{}
	k.Put(cli)
	k.Get(cli, "sample_key")
	k.Do(cli)
	k.Get(cli, "put-key")
	k.Compact(cli)
	k.Delete(cli, "sample")
	k.GetWithRev(cli)
	k.GetSortedPrefix(cli)
	k.PutErrorHandling(cli)
	//txn(cli)
	k.ChangeKey(cli)
}

func authTest(cli *clientv3.Client)  {
	a := &auth.Auth{Cli: cli, TlsConfig: tlsConfig}
	a.AuthExample()
}

func cliTest()  {
	c := client.MyClient{TlsConfig:tlsConfig}
	c.Client()
}

func leaseTest(cli *clientv3.Client)  {
	l := lease.Lease{Cli:cli}
	l.LeaseExample()
}

func watchTest(cli *clientv3.Client)  {
	w := watch.Watch{Cli:cli}
	w.WatchExample()
}

func electionTest(client *clientv3.Client)  {
	e := election.Election{Cli: client}
	e.ElectionExample()
}

func transferTest(cli *clientv3.Client)  {
	t := transfer.Transfer{Cli:cli}
	t.BalancesTransfer()
}

func lockTest(cli *clientv3.Client)  {
	l := lock.Lock{Cli:cli}
	//l.LockExample()
	l.LockWithLease()
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

