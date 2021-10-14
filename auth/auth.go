package auth

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/transport"
	"log"
	"strings"

	"time"
)

var(
	dialTimeout = 5 * time.Second
	endpoints = []string{"172.20.42.70:2379"}
)

type Auth struct {
	Cli  *clientv3.Client
	TlsInfo  transport.TLSInfo
	TlsConfig  *tls.Config
}

func (a *Auth)AuthExample()  {
	//若已存在某个role，程序重启后无法修改该role的权限，需要先删除，再创建，才可修改
	//_, _ = cli.RoleDelete(context.TODO(), "r-role")

	fmt.Println("example")
	defer a.Cli.AuthDisable(context.TODO())
	a.NormalUser()
	a.Root()
}

func (a *Auth)NormalUser()  {
	a.AddUser("user1", "123")
	a.AddRole("role1")
	a.UserBindRole("user1", "role1")

	// role1权限是可以读/写key为["foo", "goo")范围内的数据，r用户与r-role绑定，所以r用户拥有这个权限
	a.GrantPermission("role1", "foo", "goo", clientv3.PermissionType(clientv3.PermReadWrite))

	a.Cli.AuthEnable(context.TODO())

	// 普通用户连接
	cliAuth, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
		TLS:         a.TlsConfig,
		Username:    "user1",
		Password:    "123",
	})
	if err != nil {
		log.Fatal("client new ", err)
	}
	defer cliAuth.Close()
	if _, err = cliAuth.Put(context.TODO(), "foo", "bar"); err != nil {
		fmt.Println("r Put foo bar", err)
	}else {
		fmt.Println("r Put foo bar success")
	}

	// 为角色移除某个 key 的权限，此处的key和rangeEnd必须和前面创建的时候一致，即都为foo, goo
	_, err = a.Cli.RoleRevokePermission(context.TODO(), "role1", "foo", "goo")
	fmt.Println("RoleRevokePermission", err)

	// 此时已无权限
	if _, err = cliAuth.Put(context.TODO(), "foo", "bar"); err != nil {
		fmt.Println("r Put foo bar", err)
	}else {
		fmt.Println("r Put foo bar success")
	}

	// 普通用户无权限关闭认证
	_, err = cliAuth.AuthDisable(context.TODO())
	fmt.Println("user cliAuth.AuthDisable ", err)
}

// root拥有所有权限，会自动识别root这个关键字
func (a *Auth)Root()  {
	a.AddUser("root", "123")
	a.AddRole("root")
	a.UserBindRole("root", "root")
	rootCli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
		TLS:         a.TlsConfig,
		Username:    "root",
		Password:    "123",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer rootCli.Close()

	resp, err := rootCli.RoleGet(context.TODO(), "role1")
	if err != nil {
		fmt.Println("root get role", err)
	}
	fmt.Printf("user r permission: key %q, range end %q\n", resp.Perm, resp.Perm)
}


func (a *Auth)IsAlreadyExists(err error) bool {
	if strings.Contains(fmt.Sprintf("%s", err), "already exists"){
		return true
	}
	return false
}

func (a *Auth) AddUser(user, passwd string) {
	if _, err := a.Cli.UserAdd(context.TODO(), user, passwd); err != nil {
		if !a.IsAlreadyExists(err){
			log.Fatal("UserAdd ", err)
		}
	}
}

func (a *Auth) AddRole(role string)  {
	if _, err := a.Cli.RoleAdd(context.TODO(), role); err != nil {
		if !a.IsAlreadyExists(err){
			log.Fatal("RoleAdd ", err)
		}
	}
}

func (a *Auth)UserBindRole(user, role string)  {
	if _, err := a.Cli.UserGrantRole(context.TODO(), user, role); err != nil {
		if !a.IsAlreadyExists(err){
			log.Fatal("UserGrantRole ", err)
		}
	}
}


func (a *Auth)GrantPermission(user, key, rangeEnd string, permission clientv3.PermissionType)  {
	if resp, err := a.Cli.RoleGrantPermission(
		context.TODO(),
		user,   // role name
		key, // key
		rangeEnd, // range end
		permission,
	); err != nil {
		log.Fatal("RoleGrantPermission ", err)
	}else {
		fmt.Printf("RoleGrantPermission resp %v\n", resp)
	}
}