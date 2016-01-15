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
	var logFile = flag.String("log_file", "sc_consumer.log", "config log file path")
	var registedGroupIds = flag.String("group_ids", "", "regitsted groupids!eg:a,b,c,it must be integers!")
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
	consumerServer := spread_compute.NewConsumer(*configPath)
	err := consumerServer.Init()
	if err != nil {
		log.Printf("init config failed! \n")
		return
	}
	log.Printf("Now we are starting consumer service!")
	go consumerServer.StartRpc()
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
