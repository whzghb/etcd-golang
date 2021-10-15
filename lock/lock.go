package lock

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"log"
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
