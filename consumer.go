package spread_compute

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"net/rpc/jsonrpc"
	"net/url"
	"strconv"
	"sync"
)

type ConsumerTask struct {
	GroupId    int64
	Address    string
	RpcPort    int
	Name       string
	Producer   *ProducerTask `json:"-"`
	Data       []interface{}
	IsFinished bool
}

type ConsumerServer struct {
	Cg         *Config
	ConfigPath string
	Address    string
	RpcPort    int
	lock       *sync.Mutex
}

func (c *ConsumerServer) Init() error {
	config, err := InitConfig(c.ConfigPath)
	if err != nil {
		log.Printf("Consumer init failed!err:%s \n", err)
		return err
	}
	if c.lock == nil {
		c.lock = &sync.Mutex{}
	}
	c.lock.Lock()
	c.Cg = config
	c.RpcPort, err = strconv.Atoi(config.ClientRpcPort)
	if err != nil {
		log.Printf("invalid rpc port!err:%s \n", err)
		return err
	}
	c.Address = config.ClientIp
	c.lock.Unlock()
	return nil
}

func (c *ConsumerServer) StartRegist(groupId int64) error {
	resp, err := http.PostForm(fmt.Sprintf("http://%s:%s/consumer_regist", c.Cg.ListenIp, c.Cg.HttpListenIpPort),
		url.Values{
			"group_id": {fmt.Sprintf("%d", groupId)},
			"name":     {""},
			"port":     {fmt.Sprintf("%d", c.RpcPort)},
		})
	if err != nil {
		log.Printf("consuer http port failed! err:%s \n", err)
		return err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		log.Printf("consumer regist failed! status:%s \n", resp.StatusCode)
		return fmt.Errorf("consumer regist failed! status:%s \n", resp.StatusCode)
	}
	return nil
}

func (c *ConsumerServer) StartRpc() error {
	server := rpc.NewServer()
	err := c.RegistFunctions(server)
	if err != nil {
		log.Printf("consumer regist failed! \n")
		return err
	}
	server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	log.Printf("Start to listen consumer rpc!%s \n", fmt.Sprintf("%s:%d", c.Address, c.RpcPort))
	l, e := net.Listen("tcp", fmt.Sprintf("%s:%d", c.Address, c.RpcPort))
	if e != nil {
		log.Printf("consumer listen failed err:%s! \n", e)
		return e
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("accept err:%s \n", err)
			return err
		}
		go server.ServeCodec(jsonrpc.NewServerCodec(conn))
	}
	return nil
}

func (c *ConsumerServer) RegistFunctions(server *rpc.Server) error {
	err := server.Register(new(Call))
	if err != nil {
		log.Printf("Consumer regist object failed! err:%s \n", err)
		return err
	}
	return nil
}

func NewConsumer(configPath string) *ConsumerServer {
	instanceLock.Lock()
	defer instanceLock.Unlock()
	p := &ConsumerServer{ConfigPath: configPath}
	p.Init()
	CS = p
	return p
}
