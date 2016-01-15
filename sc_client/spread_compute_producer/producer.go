package main

import (
	"flag"
	"fmt"
	"gzlog"
	"log"
	"spread_compute"
	"strconv"
	"strings"
)

func main() {
	waiting := make(chan bool)
	var help = flag.String("h", "", "tools to split data,please go to github address get ")
	var configPath = flag.String("config", "", "config service path")
	var logFile = flag.String("log_file", "sc_producer.log", "config log file path")
	var registedGroupIds = flag.String("group_ids", "", "regitsted groupids!eg:a,b,c")
	flag.Parse()
	if *help == "h" {
		fmt.Println("flag.Args")
		return
	}
	if *registedGroupIds == "" {
		fmt.Println("Please pre set groupIds to regist to master!")
		return
	}
	gzlog.InitGZLogger(*logFile, 50*1000*1000, 5)
	nps := spread_compute.NewProducerServer(*configPath)
	log.Printf("Start to start rpc service!")
	go nps.StartRpc()
	log.Printf("Start to regist producer service!")
	for _, v := range strings.Split(*registedGroupIds, ",") {
		gId, err := strconv.Atoi(v)
		if err != nil {
			fmt.Printf("invalid groupid err:%s \n", err)
			return
		}
		err = nps.StartRegist(int64(gId))
		if err != nil {
			log.Printf("groupId %d regis failed !err:%s\n", gId, err)
			return
		}
	}
	fmt.Println("producer start rpc and regist suceess!")
	<-waiting
}
