package lock

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"log"
	"time"
)

type Lock struct {
	Cli   *clientv3.Client
}

func (l *Lock)LockExample()  {
	// create two separate sessions for lock competition
	s1, err := concurrency.NewSession(l.Cli)
	if err != nil {
		log.Fatal(err)
	}

	defer s1.Close()
	m1 := concurrency.NewMutex(s1, "/my-lock/")

	s2, err := concurrency.NewSession(l.Cli)
	if err != nil {
		log.Fatal(err)
	}
	defer s2.Close()
	m2 := concurrency.NewMutex(s2, "/my-lock/")

	// acquire lock for s1， 先让s1获取到锁
	if err := m1.Lock(context.TODO()); err != nil {
		log.Fatal(err)
	}
	fmt.Println("acquired lock for s1")

	m2Locked := make(chan struct{})
	go func() {
		defer close(m2Locked)
		// wait until s1 is locks /my-lock/，阻塞直到取到锁
		if err := m2.Lock(context.TODO()); err != nil {
			log.Fatal(err)
		}
	}()
	// s1释放锁让s2去获取
	if err := m1.Unlock(context.TODO()); err != nil {
		log.Fatal(err)
	}
	fmt.Println("released lock for s1")

	// 等待防止进程退出
	<-m2Locked
	fmt.Println("acquired lock for s2")
}


func (l *Lock)LockWithLease()  {
	fmt.Println("lock with lease")
	// 监听/foobar事件
	watcher := clientv3.NewWatcher(l.Cli)
	channel := watcher.Watch(context.Background(), "leaseLock2", clientv3.WithPrefix())
	go func() {
		for {
			select {
			case change := <-channel:
				for _, ev := range change.Events {
					log.Printf("etcd change on key; %s, type = %v", string(ev.Kv.Key), ev.Type)
				}
			}
		}
	}()

	go l.LockKey(1, 5)
	time.Sleep(1*time.Second)
	go l.LockKey(2, 5)
	time.Sleep(30*time.Second)
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
	//defer s.Close()
	//defer s.Orphan()
	m := concurrency.NewMutex(s, "leaseLock2")
	// acquire lock for s1
	fmt.Println("WAIT LOCK", id)
	if err := m.Lock(context.Background()); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("acquired lock for s%d\n", id)
	end := time.Now().Unix()
	fmt.Printf("---------- %d\n", end-now)
	//time.Sleep(10*time.Second)

}