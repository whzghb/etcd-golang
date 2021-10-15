package transfer

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"log"
	"math/rand"
	"sync"
)

type Transfer struct {
	Cli *clientv3.Client
}

func (t *Transfer)BalancesTransfer()  {
	// set up "accounts",生成5条数据
	totalAccounts := 5
	for i := 0; i < totalAccounts; i++ {
		k := fmt.Sprintf("accts/%d", i)
		if _, err := t.Cli.Put(context.TODO(), k, "100"); err != nil {
			log.Fatal(err)
		}
	}

	exchange := func(stm concurrency.STM) error {
		// 随机两个数之间交易
		from, to := rand.Intn(totalAccounts), rand.Intn(totalAccounts)
		if from == to {
			// nothing to do
			return nil
		}
		// read values
		fromK, toK := fmt.Sprintf("accts/%d", from), fmt.Sprintf("accts/%d", to)
		fromV, toV := stm.Get(fromK), stm.Get(toK)
		fromInt, toInt := 0, 0
		// 存的是字符串，转为int
		fmt.Sscanf(fromV, "%d", &fromInt)
		fmt.Sscanf(toV, "%d", &toInt)

		// transfer amount，将原数据的一半转移给接收者
		xfer := fromInt / 2
		fromInt, toInt = fromInt-xfer, toInt+xfer

		// write back
		stm.Put(fromK, fmt.Sprintf("%d", fromInt))
		stm.Put(toK, fmt.Sprintf("%d", toInt))
		return nil
	}

	// concurrently exchange values between accounts
	var wg sync.WaitGroup
	wg.Add(10)
	// 模拟10次随机数据转移
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			// 每一次数据交换都属于原子操作
			if _, serr := concurrency.NewSTM(t.Cli, exchange); serr != nil {
				log.Fatal(serr)
			}
		}()
	}
	wg.Wait()

	// confirm account sum matches sum from beginning. 10次随机数据转移后和还是500
	sum := 0
	accts, err := t.Cli.Get(context.TODO(), "accts/", clientv3.WithPrefix())
	if err != nil {
		log.Fatal(err)
	}
	for _, kv := range accts.Kvs {
		v := 0
		fmt.Sscanf(string(kv.Value), "%d", &v)
		sum += v
	}

	fmt.Println("account sum is", sum)
}
