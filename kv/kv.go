package kv


import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	"log"
	"time"
)

type KV struct {
}

var (
	requestTimeout = time.Duration(3*time.Second)
)

func (kv *KV)Put(cli *clientv3.Client)  {

	//cli.Delete(context.TODO(), "qwe")

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	resp, err := cli.Put(ctx, "qwe", "qw_value2")
	cancel()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(resp)
	fmt.Println("current revision:", resp.Header.Revision) // revision start at 1


	resp, err = cli.Put(context.TODO(), "qwer", "qw_value2")
	cancel()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(resp)
	fmt.Println("current revision:", resp.Header.Revision) // revision start at 1


	rsp, err := cli.Get(context.TODO(), "qw", clientv3.WithPrefix())
	fmt.Println(rsp, err)
	// current revision: 2
}

func (kv *KV)Get(cli *clientv3.Client, key string)  {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	//resp, err := cli.Get(ctx, key, clientv3.WithPrefix())
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
func (kv *KV)Do(cli *clientv3.Client)  {
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
func (kv *KV)Compact(cli *clientv3.Client)  {
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
	kv.Get(cli, "sample_key")
	kv.Get(cli, "foo")
}

func (kv *KV)Delete(cli *clientv3.Client, key string)  {
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
	kv.Get(cli, "sample_key")
}

// 通过修订版本获取值
func (kv *KV)GetWithRev(cli *clientv3.Client)  {
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
func (kv *KV)GetSortedPrefix(cli *clientv3.Client)  {
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
func (kv *KV)PutErrorHandling(cli *clientv3.Client)  {
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
func (kv *KV)Txn(cli *clientv3.Client) bool {

	key := "key"

	k, _ := cli.Get(context.TODO(), key)
	fmt.Println(k)

	// 模拟耗时操作，等待值被其他人修改
	time.Sleep(5*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)

	var txrsp *clientv3.TxnResponse
	var err error

	put := clientv3.OpPut(key, "XYZ")
	if len(k.Kvs) == 0{
		// CreateRevision是这个key创建时被分配的这个序号，当key不存在时，createRevision是0
		cmpExists := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
		txrsp, err = cli.Txn(ctx).If(cmpExists).Then(put).Else().Commit()
	}else {
		// ModRevision是修改的revision，每修改一次值加1，如果当前的revision和前面获取到的revision一致，说明没有被修改
		cmpModVersion := clientv3.Compare(clientv3.ModRevision(key), "=", k.Kvs[0].ModRevision)
		txrsp, err = cli.Txn(ctx).If(cmpModVersion).Then(put).Else().Commit()
	}

	cancel()
	if err != nil {
		log.Fatal(err)
	}
	// 如果上面IF判断成功，succeeded为true，否则为false
	fmt.Println(txrsp.Succeeded)

	// 模拟值被修改后此处为omg，否则为XYZ
	kv.Get(cli, key)
	return txrsp.Succeeded
}

func (kv *KV)ChangeKey(cli *clientv3.Client)  {
	go func() {
		time.Sleep(2*time.Second)
		rsp, err := cli.Put(context.TODO(), "key", "omg")
		fmt.Println(rsp, err)
		fmt.Println("change to omg")
	}()
	for{
		succeed := kv.Txn(cli)
		// 修改成功则退出
		if succeed{
			break
		}
		// 否则一秒后重新尝试
		time.Sleep(1*time.Second)
	}
}

func (k *KV)LockExample(cli *clientv3.Client)  {
	go k.LockT(cli)
	time.Sleep(1*time.Second)
	go k.LockT(cli)
	time.Sleep(1*time.Second)
	k.LockT(cli)
}

func (k *KV)LockT(cli *clientv3.Client)  {
	pfx:="mylock"
	lease, err  := cli.Grant(context.Background(), 10)
	if err != nil{
		return
	}
	myKey := fmt.Sprintf("%s%d", pfx, lease.ID)
	cmp := clientv3.Compare(clientv3.CreateRevision(myKey), "=", 0)
	// put self in lock waiters via myKey; oldest waiter holds lock
	put := clientv3.OpPut(myKey, "", clientv3.WithLease(lease.ID))
	// reuse key in case this session already holds the lock
	get := clientv3.OpGet(myKey)
	// fetch current holder to complete uncontended path with only one RPC
	getOwner := clientv3.OpGet(pfx, clientv3.WithFirstCreate()...)
	resp, err := cli.Txn(context.TODO()).If(cmp).Then(put, getOwner).Else(get, getOwner).Commit()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(resp.Succeeded)
	myRev := resp.Header.Revision
	if !resp.Succeeded {
		myRev = resp.Responses[0].GetResponseRange().Kvs[0].CreateRevision
	}
	fmt.Println("myrev, header", myRev, resp.Header.Revision)

	// if no key on prefix / the minimum rev is key, already hold the lock
	ownerKey := resp.Responses[1].GetResponseRange().Kvs
	fmt.Println(ownerKey)
	if len(ownerKey) == 0 || ownerKey[0].CreateRevision == myRev {
		hdr := resp.Header
		fmt.Println("---------", hdr)
		return
	}
	// wait for deletion revisions prior to myKey
	go func() {
		cli.Put(context.TODO(), "a", "b")
	}()
	hdr, werr := waitDeletes(context.TODO(), cli, pfx, myRev-1)
	// release lock key if wait failed
	if werr != nil {
		cli.Delete(context.TODO(), myKey)
	} else {
		hdr = hdr
		fmt.Println(hdr)
	}
}

func waitDelete(ctx context.Context, client *clientv3.Client, key string, rev int64) error {
	fmt.Println("wait delete rev", rev)
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wr clientv3.WatchResponse
	wch := client.Watch(cctx, key, clientv3.WithRev(rev))
	for wr = range wch {
		for _, ev := range wr.Events {
			if ev.Type == mvccpb.DELETE {
				fmt.Println("deleted")
				return nil
			}
		}
	}
	if err := wr.Err(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("lost watcher waiting for delete")
}

// waitDeletes efficiently waits until all keys matching the prefix and no greater
// than the create revision.
func waitDeletes(ctx context.Context, client *clientv3.Client, pfx string, maxCreateRev int64) (*pb.ResponseHeader, error) {
	getOpts := append(clientv3.WithLastCreate(), clientv3.WithMaxCreateRev(maxCreateRev))
	for {
		resp, err := client.Get(ctx, pfx, getOpts...)
		fmt.Println("get resp", resp)
		if err != nil {
			return nil, err
		}
		if len(resp.Kvs) == 0 {
			return resp.Header, nil
		}
		lastKey := string(resp.Kvs[0].Key)
		if err = waitDelete(ctx, client, lastKey, resp.Header.Revision); err != nil {
			return nil, err
		}
	}
}

