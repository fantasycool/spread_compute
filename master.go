package spread_compute

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"net/rpc/jsonrpc"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	globalCache = &GlobalCache{}
	CacheMutex  = &sync.Mutex{}
	ShutDown    = make(chan bool)
)

type MasterServer struct {
	IsInit     bool
	IsStart    bool
	Cg         *Config
	lock       *sync.Mutex
	ConfigPath string
}

func (m *MasterServer) RpcServeRequest() error {
	server := rpc.NewServer()
	err := m.RegistFunction(server)
	if err != nil {
		log.Printf("regist function error,err :%s \n", err)
		return err
	}
	server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	log.Printf("Start to listen server, m.Cg.ListenIpPort %s \n", m.Cg.ListenIpPort)
	l, e := net.Listen("tcp", ":"+m.Cg.ListenIpPort)
	if e != nil {
		log.Printf("Listen tcp error! %s \n", e)
		return e
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("Accept request error! err:%s \n", err)
			return err
		}
		go server.ServeCodec(jsonrpc.NewServerCodec(conn))
	}
	return nil
}

func (m *MasterServer) HttpServRequest() error {
	http.HandleFunc("/consumer_regist", ConsumerRegist)
	http.HandleFunc("/producer_regist", ProducerRegist)
	http.HandleFunc("/start_task", StartTaskRegistAndExec)
	log.Printf("Start to listen http listen port %s \n", m.Cg.HttpListenIpPort)
	err := http.ListenAndServe(":"+m.Cg.HttpListenIpPort, nil)
	if err != nil {
		log.Printf("http listen failed!err:%s \n", err)
		return err
	} else {
		log.Printf("http listen success")
	}
	return nil
}

func (m *MasterServer) RegistFunction(server *rpc.Server) error {
	err := rpc.Register(globalCache)
	if err != nil {
		log.Printf("regist failed! err:%s \n", err)
	}
	return err
}

func (m *MasterServer) Init() error {
	if m.lock == nil {
		m.lock = &sync.Mutex{}
	}
	m.lock.Lock()
	config, err := InitConfig(m.ConfigPath)
	m.Cg = config
	m.IsInit = true
	m.lock.Unlock()
	if err != nil {
		log.Printf("master init config file failed! err:%s \n", err)
		return err
	}
	return nil
}

func (m *MasterServer) Start() error {
	log.Printf("Start to call start!")
	if !m.IsInit {
		return fmt.Errorf("it must be inited first!")
	}
	log.Printf("Start to serve http request~!")
	go m.HttpServRequest()
	log.Printf("Start to serve rpc request~!")
	go m.RpcServeRequest()
	log.Printf("Now we start to iterate our caches and find the group that need to do work!")
	go QueryOkConnector(m)
	<-ShutDown
	log.Printf("get shutdown command,so we shutdown now!")
	return nil
}

//before calling this function, we can do somethong interesting!like crontab!
func CallProducerSyncAndExec(groupId int64) error {
	connectorCache := globalCache.Ccaches[groupId]
	if !connectorCache.IsReady {
		return fmt.Errorf("producers and consumers is not ready!")
	} else {
		for _, p := range connectorCache.ProducerList {
			go func() error {
				log.Printf("Start to dial producer address:%s rpc;%d \n", p.Address, p.RpcPort)
				client, err := jsonrpc.Dial("tcp", fmt.Sprintf("%s:%d", p.Address, p.RpcPort))
				if err != nil {
					log.Printf("Dial producer client failed! err:%s \n", err)
					return err
				}
				sentConsumers := make([]ConsumerTask, 0)
				for _, s := range p.Consumers {
					sentConsumers = append(sentConsumers, *s)
				}
				var result *Status = new(Status)

				args := &SyncConsumerArgs{GroupId: groupId, Consumers: sentConsumers}
				log.Printf("Start to call sync consumers!consumers is %s \n", sentConsumers)
				err = client.Call("PMSync.SyncConsumer", args, result)
				if err != nil {
					log.Printf("call sync consumers failed! err:%s \n", err)
					return err
				}
				log.Printf("Call sync success ! \n")

				log.Printf("Start to call exec method !\n")
				commonArgs := &CommonFunctionCallArgs{GroupId: groupId, CallType: "producer"}
				var execResult *Status = new(Status)
				err = client.Call("Call.CallFunction", commonArgs, execResult)
				if err != nil {
					log.Printf("Call call function failed! err:%s \n", err)
					return err
				}
				log.Printf("Call sync success! \n")

				return nil
			}()
		}
	}
	return nil
}

func StartTaskRegistAndExec(resp http.ResponseWriter, req *http.Request) {
	groupId := req.FormValue("group_id")
	if globalCache.Ccaches == nil {
		resp.WriteHeader(http.StatusNotFound)
		return
	}
	if groupId == "" {
		resp.WriteHeader(http.StatusNotFound)
		return
	} else {
		if gId, err := strconv.Atoi(groupId); err != nil {
			log.Printf("convert groupid failed! err:%s \n", err)
		} else {
			err := CallProducerSyncAndExec(int64(gId))
			if err != nil {
				log.Printf("Call group failed! groupId %d , err:%s", gId, err)
			}
			log.Printf("Call group to do work success!")
			resp.WriteHeader(http.StatusOK)
		}
	}
}

func ProducerRegist(resp http.ResponseWriter, req *http.Request) {
	groupId := req.FormValue("group_id")
	name := req.FormValue("name")
	port, err := strconv.Atoi(req.FormValue("port"))
	if err != nil {
		resp.WriteHeader(http.StatusNotAcceptable)
		return
	}
	if groupId == "" {
		resp.WriteHeader(http.StatusNotAcceptable)
	} else {
		if g, err := strconv.Atoi(groupId); err != nil {
			resp.WriteHeader(http.StatusNotAcceptable)
		} else {
			remoteAddr := req.RemoteAddr
			CacheMutex.Lock()
			if globalCache.Ccaches == nil {
				globalCache.Ccaches = make(map[int64]*ConnectorCache)
			}
			if globalCache.Ccaches[int64(g)] == nil {
				globalCache.Ccaches[int64(g)] = &ConnectorCache{IsFinished: false, IsReady: false}
			}
			if globalCache.Ccaches[int64(g)].ProducerList == nil {
				globalCache.Ccaches[int64(g)].ProducerList = make([]*ProducerTask, 0)
			}
			for _, i := range globalCache.Ccaches[int64(g)].ProducerList {
				if i.Address == remoteAddr {
					resp.WriteHeader(http.StatusCreated)
					return
				}
			}
			address := strings.Split(remoteAddr, ":")[0]
			dlock.Lock()
			if IfExists(address, strconv.Itoa(port)) {
				log.Printf("address:%s,port:%s already exists!", address, strconv.Itoa(port))
				return
			}
			distinctCaches[fmt.Sprintf("%s:%s", address, strconv.Itoa(port))] = true
			globalCache.Ccaches[int64(g)].ProducerList = append(globalCache.Ccaches[int64(g)].ProducerList,
				&ProducerTask{GroupId: int64(g), Address: address, Name: name, RpcPort: port})
			dlock.Unlock()
			CacheMutex.Unlock()
		}
	}
}

var distinctCaches = make(map[string]bool)
var dlock = sync.Mutex{}

func IfExists(address string, port string) bool {
	key := fmt.Sprintf("%s:%s", address, port)
	_, b := distinctCaches[key]
	return b
}

func ConsumerRegist(resp http.ResponseWriter, req *http.Request) {
	log.Printf("Start to call consumer regist \n")
	groupId := req.FormValue("group_id")
	name := req.FormValue("name")
	port, err := strconv.Atoi(req.FormValue("port"))
	if err != nil {
		resp.WriteHeader(http.StatusNotAcceptable)
		return
	}
	if groupId == "" {
		resp.WriteHeader(http.StatusNotAcceptable)
	} else {
		if g, err := strconv.Atoi(groupId); err != nil {
			resp.WriteHeader(http.StatusNotAcceptable)
		} else {
			remoteAddr := req.RemoteAddr
			log.Printf("Start to lock!")
			CacheMutex.Lock()
			defer CacheMutex.Unlock()
			if globalCache.Ccaches == nil {
				globalCache.Ccaches = make(map[int64]*ConnectorCache)
			}
			if globalCache.Ccaches[int64(g)] == nil {
				globalCache.Ccaches[int64(g)] = &ConnectorCache{IsReady: false, IsFinished: false}
			}
			if globalCache.Ccaches[int64(g)].ConsumerList == nil {
				globalCache.Ccaches[int64(g)].ConsumerList = make([]*ConsumerTask, 0)
				globalCache.Ccaches[int64(g)].StartTime = time.Now()
			}
			for _, i := range globalCache.Ccaches[int64(g)].ConsumerList {
				if i.Address == remoteAddr {
					resp.WriteHeader(http.StatusCreated)
					return
				}
			}
			address := strings.Split(remoteAddr, ":")[0]
			dlock.Lock()
			if IfExists(address, strconv.Itoa(port)) {
				log.Printf("address, port already exists!")
				return
			}
			distinctCaches[fmt.Sprintf("%s:%s", address, strconv.Itoa(port))] = true
			globalCache.Ccaches[int64(g)].ConsumerList = append(globalCache.Ccaches[int64(g)].ConsumerList,
				&ConsumerTask{GroupId: int64(g), Address: address, Name: name, RpcPort: port})
			log.Printf("now groupId %s consumerList length is %d, content is %s \n", groupId,
				len(globalCache.Ccaches[int64(g)].ConsumerList),
				globalCache.Ccaches[int64(g)].ConsumerList)
			dlock.Unlock()
		}
	}
}
