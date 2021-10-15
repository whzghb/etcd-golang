package election

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"log"
	"sync"
	"time"
)

type Election struct {
	Cli  *clientv3.Client
}

func (el *Election)ElectionExample()  {
	// create three separate sessions for election competition
	s1, err := concurrency.NewSession(el.Cli)
	if err != nil {
		log.Fatal(err)
	}
	defer s1.Close()
	e1 := concurrency.NewElection(s1, "/my-election/")

	s2, err := concurrency.NewSession(el.Cli)
	if err != nil {
		log.Fatal(err)
	}
	defer s2.Close()
	e2 := concurrency.NewElection(s2, "/my-election/")

	s3, err := concurrency.NewSession(el.Cli)
	if err != nil {
		log.Fatal(err)
	}
	defer s3.Close()
	e3 := concurrency.NewElection(s3, "/my-election/")


	// create competing candidates, with e1 initially losing to e2 or e3
	var wg sync.WaitGroup
	wg.Add(3)
	electc := make(chan *concurrency.Election, 3)
	go func() {
		defer wg.Done()
		// delay candidacy so e2 wins first
		time.Sleep(3 * time.Second)
		if err := e1.Campaign(context.Background(), "e1"); err != nil {
			log.Fatal(err)
		}
		electc <- e1
	}()
	go func() {
		defer wg.Done()
		if err := e2.Campaign(context.Background(), "e2"); err != nil {
			log.Fatal(err)
		}
		electc <- e2
	}()

	go func() {
		defer wg.Done()
		// 一直阻塞直到被选举上或者发生错误或者context cancel掉
		if err := e3.Campaign(context.Background(), "e3"); err != nil {
			log.Fatal(err)
		}
		electc <- e3
	}()

	cctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	e := <-electc
	fmt.Println("completed first election with", string((<-e.Observe(cctx)).Kvs[0].Value))  // e2或者e3

	// resign so next candidate can be elected, 重新开始选举
	if err := e.Resign(context.TODO()); err != nil {
		log.Fatal(err)
	}

	e = <-electc
	fmt.Println("completed second election with", string((<-e.Observe(cctx)).Kvs[0].Value)) // e2或者e3

	// resign so next candidate can be elected, 重新开始选举
	if err := e.Resign(context.TODO()); err != nil {
		log.Fatal(err)
	}

	e = <-electc
	fmt.Println("completed second election with", string((<-e.Observe(cctx)).Kvs[0].Value)) // e1

	wg.Wait()
}