package lease

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"log"
	"time"
)

type Lease struct {
	Cli *clientv3.Client
}

func (l *Lease)LeaseExample()  {
	//l.grant()
	//l.keepAlived()
	//l.keepAliveOnce()
	l.revoke()
}

func (l *Lease)grant()  {
	// minimum lease TTL is 5-second  最小租约为5秒，设为1也没用
	resp, err := l.Cli.Grant(context.TODO(), 1)
	if err != nil {
		log.Fatal(err)
	}
	// after 5 seconds, the key 'aa' will be removed
	_, err = l.Cli.Put(context.TODO(), "aa", "bar", clientv3.WithLease(resp.ID))
	if err != nil {
		log.Fatal(err)
	}

	rsp, _ := l.Cli.Get(context.TODO(), "aa")
	fmt.Println(rsp.Kvs)

	time.Sleep(10*time.Second)

	// 此时key aa被删除了
	rsp, _ = l.Cli.Get(context.TODO(), "aa")
	fmt.Println("it is none", rsp.Kvs)
}

func (l *Lease)keepAlived()  {
	// 申请5秒的租约
	resp, err := l.Cli.Grant(context.TODO(), 5)
	if err != nil {
		log.Fatal(err)
	}

	_, err = l.Cli.Put(context.TODO(), "foo", "bar", clientv3.WithLease(resp.ID))
	if err != nil {
		log.Fatal(err)
	}

	// the key 'foo' will be kept forever, foo会被永久保存
	ch, kaerr := l.Cli.KeepAlive(context.TODO(), resp.ID)
	if kaerr != nil {
		log.Fatal(kaerr)
	}
	fmt.Println(ch)
	ka := <-ch
	fmt.Println("ttl:", ka.TTL)

	time.Sleep(10*time.Second)
	rsp, _ := l.Cli.Get(context.TODO(), "foo")
	fmt.Println(rsp)
}

func (l *Lease)keepAliveOnce()  {
	resp, err := l.Cli.Grant(context.TODO(), 5)
	if err != nil {
		log.Fatal(err)
	}

	_, err = l.Cli.Put(context.TODO(), "foo", "bar", clientv3.WithLease(resp.ID))
	if err != nil {
		log.Fatal(err)
	}

	// to renew the lease only once， 只会续约一次
	ka, kaerr := l.Cli.KeepAliveOnce(context.TODO(), resp.ID)
	if kaerr != nil {
		log.Fatal(kaerr)
	}

	fmt.Println("ttl:", ka.TTL)

	// 还在
	time.Sleep(7*time.Second)
	rsp, _ := l.Cli.Get(context.TODO(), "foo")
	fmt.Println(rsp)

	fmt.Println(ka.TTL)

	// 还在
	time.Sleep(1*time.Second)
	rsp, _ = l.Cli.Get(context.TODO(), "foo")
	fmt.Println(rsp)

	fmt.Println(ka.TTL)
	// 已删除
	time.Sleep(3*time.Second)
	rsp, _ = l.Cli.Get(context.TODO(), "foo")
	fmt.Println(rsp)

	fmt.Println(ka.TTL)
}

func (l *Lease) revoke()  {
	resp, err := l.Cli.Grant(context.TODO(), 5)
	if err != nil {
		log.Fatal(err)
	}

	_, err = l.Cli.Put(context.TODO(), "foo", "bar", clientv3.WithLease(resp.ID))
	if err != nil {
		log.Fatal(err)
	}

	gresp, err := l.Cli.Get(context.TODO(), "foo")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("number of keys:", len(gresp.Kvs))

	// revoking lease expires the key attached to its lease ID, 使租约失效，立即删除key
	rsp, err := l.Cli.Revoke(context.TODO(), resp.ID)
	if err != nil {
		log.Fatal(err)
	}else {
		fmt.Println(rsp)
	}

	gresp, err = l.Cli.Get(context.TODO(), "foo")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("number of keys:", len(gresp.Kvs))
}