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

type ProducerTask struct {
	GroupId         int64
	RpcPort         int
	Address         string
	Name            string
	IsMasterReady   bool
	IsConsumerReady bool
	IsProducerReady bool
	IsRpcStart      bool
	IsShutDown      bool
	Consumers       []*ConsumerTask
	ConfigPath      string
}

type ProducerServer struct {
	RpcPort    int
	Address    string
	IsShutDown bool
	IsRpcStart bool
	ConfigPath string
	Cg         *Config
	lock       *sync.Mutex
}

func (p *ProducerServer) StartRegist(groupId int64) error {
	log.Println("Start to regist producer...")
	log.Printf("http://%s:%s/producer_regist \n", p.Cg.ListenIp, p.Cg.HttpListenIpPort)
	resp, err := http.PostForm(fmt.Sprintf("http://%s:%s/producer_regist", p.Cg.ListenIp, p.Cg.HttpListenIpPort),
		url.Values{
			"group_id": {fmt.Sprintf("%d", groupId)},
			"name":     {""},
			"port":     {fmt.Sprintf("%d", p.RpcPort)},
		})
	if err != nil {
		log.Printf("consume http port failed! err:%s \n", err)
		return err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		log.Printf("producer regist failed! status:%s \n", resp.StatusCode)
		return fmt.Errorf("producer regist failed! status:%s \n", resp.StatusCode)
	}
	return nil
}

func (c *ProducerServer) StartRpc() error {
	log.Printf("Start to start rpc! \n")
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
		if c.IsShutDown {
			break
		}
		conn, err := l.Accept()
		if err != nil {
			log.Printf("accept err:%s \n", err)
			return err
		}
		go server.ServeCodec(jsonrpc.NewServerCodec(conn))
	}
	return nil
}

func (p *ProducerServer) RegistFunctions(server *rpc.Server) error {
	err := server.Register(new(Call))
	if err != nil {
		log.Printf("register function failed!err:%s \n", err)
		return err
	}
	err = server.Register(new(PMSync))
	if err != nil {
		log.Printf("regist PMSync failed! err:%s \n", err)
		return err
	}
	return nil
}

func (p *ProducerServer) Init() error {
	if p.lock == nil {
		p.lock = &sync.Mutex{}
	}
	p.lock.Lock()
	config, err := InitConfig(p.ConfigPath)
	p.Cg = config
	p.Address = config.ServerIp
	p.RpcPort, err = strconv.Atoi(config.ServerRpcPort)
	if err != nil {
		log.Printf("config server rpc port config is not valid! err:%s \n", err)
		return err
	}
	p.lock.Unlock()
	if err != nil {
		log.Printf("Producer InitConfig failed!")
		return err
	}
	return nil
}

func (p *ProducerServer) Stop() error {
	log.Printf("Start to stop Producer!")
	ShutDown <- true
	p.IsShutDown = true
	return nil
}

func NewProducerServer(configPath string) *ProducerServer {
	instanceLock.Lock()
	defer instanceLock.Unlock()
	p := &ProducerServer{ConfigPath: configPath}
	p.Init()
	PS = p
	return p
}
