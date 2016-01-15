package main

import (
	"flag"
	"fmt"
	"gzlog"
	"log"
	"spread_compute"
)

func main() {
	waiting := make(chan bool)
	var help = flag.String("h", "", "tools to split data,please go to github address get ")
	var configPath = flag.String("config", "", "config service path")
	var logFile = flag.String("log_file", "sc_master.log", "config log file path")
	flag.Parse()
	if *help == "h" {
		fmt.Println("flag.Args")
		return
	}
	gzlog.InitGZLogger(*logFile, 50*1000*1000, 5)
	master := &spread_compute.MasterServer{ConfigPath: *configPath}
	log.Printf("Start to init Master Server")
	err := master.Init()
	if err != nil {
		log.Printf("Start master failed! \n")
		return
	}
	log.Printf("Starting master server!")
	master.Start()
	<-waiting
}
