package main

import (
	"flag"
	"fmt"
	"gzlog"
	"log"
	"os/exec"
	"spread_compute"
	"strconv"
	"strings"
)

type ConsumerServiceClient struct {
	CommandStr string
}

func (c *ConsumerServiceClient) ConsumeData(data []interface{}) error {
	log.Printf("Start to consume data!data length is %d \n", len(data))
	strIds := make([]string, 0)
	for _, d := range data {
		strIds = append(strIds, strconv.Itoa(int(d.(float64))))
	}
	pStr := strings.Join(strIds, ",")
	log.Printf("command str is %s \n", c.CommandStr)
	c.CommandStr = c.CommandStr + " " + pStr
	out, err := exec.Command(strings.Fields(c.CommandStr)[0], strings.Fields(c.CommandStr)[1:]...).Output()
	if err != nil {
		log.Printf("consumer execute command failed!err:%s \n", err)
		return err
	}
	log.Printf("execute command success!message is:%s \n", string(out))
	return nil
}

func main() {
	waiting := make(chan bool)
	var help = flag.String("h", "", "tools to split data,please go to github address get ")
	var configPath = flag.String("config", "", "config service path")
	var logFile = flag.String("log_file", "sc_consumer.log", "config log file path")
	var registedGroupIds = flag.String("group_ids", "", "regitsted groupids!eg:a,b,c,it must be integers!")
	var commandStr = flag.String("cmd", "", "system command to execute!")
	flag.Parse()
	if *help == "h" {
		fmt.Println("flag.Args")
		return
	}
	if *registedGroupIds == "" {
		fmt.Println("Please pre set groupIds to regist to master!")
		return
	}
	if *commandStr == "" {
		fmt.Println("Pleast set command str,command str cannot be nil")
		return
	}

	gzlog.InitGZLogger(*logFile, 50*1000*1000, 5)
	consumerServer := spread_compute.NewConsumer(*configPath)
	err := consumerServer.Init()
	if err != nil {
		log.Printf("init config failed! \n")
		return
	}
	log.Printf("Now we are starting consumer service!")
	go consumerServer.StartRpc()
	spread_compute.RegistConsumerService(2, &ConsumerServiceClient{*commandStr})
	for _, v := range strings.Split(*registedGroupIds, ",") {
		gId, err := strconv.Atoi(v)
		if err != nil {
			fmt.Printf("invalid groupid err:%s \n", err)
			return
		}
		err = consumerServer.StartRegist(int64(gId))
		if err != nil {
			log.Printf("consumer regist failed!\n")
		}
	}
	<-waiting
}
