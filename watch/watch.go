package watch

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"time"
)

type Watch struct {
	Cli *clientv3.Client
}

func (w *Watch)WatchExample()  {
	//w.watch()
	//w.watchWithPrefix()
	w.progressNotify()
}

func (w *Watch)watch()  {
	go func() {
		time.Sleep(1*time.Second)
		_, _ = w.Cli.Put(context.TODO(), "foo", "bar")
	}()

	go func() {
		time.Sleep(1*time.Second)
		_, _ = w.Cli.Put(context.TODO(), "foo", "bar2")
	}()

	rch := w.Cli.Watch(context.Background(), "foo")

	// 一直监听不会退出
	for wresp := range rch {
		for _, ev := range wresp.Events {
			fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
		}
	}
}


func (w *Watch)watchWithPrefix()  {
	go func() {
		time.Sleep(1*time.Second)
		_, _ = w.Cli.Put(context.TODO(), "foo1", "bar")
	}()

	go func() {
		time.Sleep(1*time.Second)
		_, _ = w.Cli.Put(context.TODO(), "foo2", "bar2")
	}()

	go func() {
		time.Sleep(1*time.Second)
		_, _ = w.Cli.Delete(context.TODO(), "foo2")
	}()

	// 前缀监听
	rch := w.Cli.Watch(context.Background(), "foo", clientv3.WithPrefix())

	//range
	//rch := cli.Watch(context.Background(), "foo1", clientv3.WithRange("foo4"))

	// 一直监听不会退出
	for wresp := range rch {
		for _, ev := range wresp.Events {
			fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
		}
	}
}

func (w *Watch) progressNotify()  {
	go func() {
		time.Sleep(1*time.Second)
		_, _ = w.Cli.Put(context.TODO(), "foo", "bar")
	}()

	go func() {
		time.Sleep(2*time.Second)
		_, _ = w.Cli.Delete(context.TODO(), "foo")
	}()

	// 进度通知
	rch := w.Cli.Watch(context.Background(), "foo", clientv3.WithProgressNotify())
	//wresp := <-rch

	for wresp := range rch {
		for _, ev := range wresp.Events {
			fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			fmt.Printf("wresp.Header.Revision: %d\n", wresp.Header.Revision)
			fmt.Println("wresp.IsProgressNotify:", wresp.IsProgressNotify())
		}
	}
}