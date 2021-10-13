package main

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	"log"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/transport"
)

var (
	dialTimeout = 5 * time.Second
	requestTimeout = time.Duration(3*time.Second)
	endpoints = []string{"172.20.42.70:2379"}
)

func main() {
	cli := getCli()
	defer cli.Close() // make sure to close the client

	put(cli)
	get(cli, "sample_key")
	do(cli)
	get(cli, "put-key")
	compact(cli)
	delete(cli, "sample")
	getWithRev(cli)
	getSortedPrefix(cli)
	putErrorHandling(cli)
	//txn(cli)
	changeKey(cli)
}

func getCli() *clientv3.Client {
	tlsInfo := transport.TLSInfo{
		CertFile:      "tls/kube-etcd-172-20-42-70.pem",
		KeyFile:       "tls/kube-etcd-172-20-42-70-key.pem",
		TrustedCAFile: "tls/kube-ca.pem",
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		log.Fatal(err)
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
		TLS:         tlsConfig,
	})
	if err != nil {
		log.Fatal(err)
	}
	return cli
}

func put(cli *clientv3.Client)  {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	resp, err := cli.Put(ctx, "sample_key", "sample_value")
	cancel()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("current revision:", resp.Header.Revision) // revision start at 1
	// current revision: 2
}

func get(cli *clientv3.Client, key string)  {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	resp, err := cli.Get(ctx, key)
	cancel()
	if err != nil {
		log.Fatal(err)
	}
	for _, ev := range resp.Kvs {
		fmt.Printf("%s : %s\n", ev.Key, ev.Value)
	}
	// foo : bar
}

// 不使用事务执行多个操作,Do 在创建任意操作时很有用
func do(cli *clientv3.Client)  {
	fmt.Println("do")
	ops := []clientv3.Op{
		clientv3.OpPut("put-key", "123"),
		clientv3.OpGet("put-key"),
		clientv3.OpGet("put-key"),
		clientv3.OpPut("put-key", "456"),
		clientv3.OpPut("aaa", "bbbbbb"),
		clientv3.OpGet("aaa"),
	}
	for _, op := range ops {
		if resp, err := cli.Do(context.TODO(), op); err != nil {
			log.Fatal(err)
		}else {
			fmt.Println(resp.Get())
		}
	}
	fmt.Println("done")
}

// 压缩
func compact(cli *clientv3.Client)  {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	resp, err := cli.Get(ctx, "put-key")
	cancel()
	if err != nil {
		log.Fatal(err)
	}
	compRev := resp.Header.Revision // specify compact revision of your choice
	ctx, cancel = context.WithTimeout(context.Background(), requestTimeout)
	_, err = cli.Compact(ctx, compRev)
	cancel()
	if err != nil {
		log.Fatal(err)
	}
	get(cli, "sample_key")
	get(cli, "foo")
}

func delete(cli *clientv3.Client, key string)  {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	// count keys about to be deleted, 根据前缀获取
	gresp, err := cli.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(gresp.Count)

	// delete the keys  根据前缀删除
	dresp, err := cli.Delete(ctx, key, clientv3.WithPrefix())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Deleted all keys:", int64(len(gresp.Kvs)) == dresp.Deleted)

    // 精确获取
	gresp, err = cli.Get(ctx, "fo")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(gresp.Count)

	// 精确删除
	dresp, err = cli.Delete(ctx, "fo")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(dresp)

	// 已被删除
	get(cli, "sample_key")
}

// 通过修订版本获取值
func getWithRev(cli *clientv3.Client)  {
	presp, err := cli.Put(context.TODO(), "foo", "bar1")
	if err != nil {
		log.Fatal(err)
	}
	_, err = cli.Put(context.TODO(), "foo", "bar2")
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	resp, err := cli.Get(ctx, "foo", clientv3.WithRev(presp.Header.Revision))
	cancel()
	if err != nil {
		log.Fatal(err)
	}
	for _, ev := range resp.Kvs {
		fmt.Printf("%s : %s\n", ev.Key, ev.Value)  //会获取到bar1，尽管已经被被修改为bar2
	}
}

// 按key排序
func getSortedPrefix(cli *clientv3.Client)  {
	for i := range make([]int, 3) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		_, err := cli.Put(ctx, fmt.Sprintf("key_%d", i), "value")
		cancel()
		if err != nil {
			log.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	// 降序
	resp, err := cli.Get(ctx, "key", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	cancel()
	if err != nil {
		log.Fatal(err)
	}
	for _, ev := range resp.Kvs {
		fmt.Printf("%s : %s\n", ev.Key, ev.Value)
	}


	ctx, cancel = context.WithTimeout(context.Background(), requestTimeout)
	// 升序
	resp, err = cli.Get(ctx, "key", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	cancel()
	if err != nil {
		log.Fatal(err)
	}
	for _, ev := range resp.Kvs {
		fmt.Printf("%s : %s\n", ev.Key, ev.Value)
	}

}

// 错误处理
func putErrorHandling(cli *clientv3.Client)  {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	_, err := cli.Put(ctx, "", "sample_value")
	cancel()
	if err != nil {
		switch err {
		case context.Canceled:
			fmt.Printf("ctx is canceled by another routine: %v\n", err)
		case context.DeadlineExceeded:
			fmt.Printf("ctx is attached with a deadline is exceeded: %v\n", err)
		case rpctypes.ErrEmptyKey:
			fmt.Printf("client-side error: %v\n", err)
		default:
			fmt.Printf("bad cluster endpoints, which are not etcd servers: %v\n", err)
		}
	}
}

// 事务
func txn(cli *clientv3.Client) bool {
	k, _ := cli.Get(context.TODO(), "key")
	fmt.Println("get key", string(k.Kvs[0].Value))
	kvc := clientv3.NewKV(cli)

	_, err := kvc.Put(context.TODO(), "key", string(k.Kvs[0].Value))
	if err != nil {
		log.Fatal(err)
	}

	// 模拟该值已被修改
	//_, _ = cli.Put(context.TODO(), "key", "omg")
	// 模拟耗时操作，等待值被其他人修改
	time.Sleep(5*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	txrsp, err := kvc.Txn(ctx).
		// ETCD中的txn通过"If-Then-Else"实现了原子操作。
		// txn value comparisons are lexical，如果开始获取到的值和现在的值一致，表示该值没有被改动，乐观锁
		If(clientv3.Compare(clientv3.Value("key"), "=", string(k.Kvs[0].Value))).
		// the "Then" runs
		Then(clientv3.OpPut("key", "XYZ")).
		// the "Else" does not run
		// clientv3.OpPut("key", "ABC")可以做操作，也可以不做操作
		Else().
		Commit()
	cancel()
	if err != nil {
		log.Fatal(err)
	}
	// 如果上面IF判断成功，succeeded为true，否则为false
	fmt.Println(txrsp.Succeeded)

	// 模拟值被修改后此处为omg，否则为XYZ
	get(cli, "key")
	return txrsp.Succeeded
}

func changeKey(cli *clientv3.Client)  {
	go func() {
		time.Sleep(2*time.Second)
		rsp, err := cli.Put(context.TODO(), "key", "omg")
		fmt.Println(rsp, err)
		fmt.Println("change to omg")
	}()
	for{
		succeed := txn(cli)
		// 修改成功则退出
		if succeed{
			break
		}
		// 否则一秒后重新尝试
		time.Sleep(1*time.Second)
	}
}