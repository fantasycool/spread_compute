package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"gzlog"
	"log"
	"spread_compute"
	"strconv"
	"strings"
)

type ProducerServiceUser struct {
	MysqlUser   string
	MysqlPasswd string
	MysqlHost   string
}

func (t *ProducerServiceUser) ProduceData() ([]interface{}, error) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@(%s)/jwlwl?charset=utf8&parseTime=true",
		t.MysqlUser, t.MysqlPasswd, t.MysqlHost))
	defer db.Close()
	rows, err := db.Query("select id from users")
	if err != nil {
		log.Printf("Producer select produce data failed!err:%s \n", err)
		return nil, err
	}
	defer rows.Close()
	result := make([]interface{}, 0)
	for rows.Next() {
		userId := 0
		err := rows.Scan(&userId)
		if err != nil {
			log.Printf("row scan to userId failed! err:%s \n", err)
			return nil, err
		}
		result = append(result, userId)
	}
	return result, nil
}

func main() {
	waiting := make(chan bool)
	var help = flag.String("h", "", "tools to split data,please go to github address get ")
	var configPath = flag.String("config", "", "config service path")
	var logFile = flag.String("log_file", "sc_producer.log", "config log file path")
	var registedGroupIds = flag.String("group_ids", "", "regitsted groupids!eg:a,b,c")
	var mysqlUser = flag.String("muser", "", "mysql user")
	var mysqlPassword = flag.String("mpass", "", "mysql password")
	var mysqlHost = flag.String("mhost", "", "mysqlhost")

	flag.Parse()
	if *help == "h" {
		fmt.Println("flag.Args")
		return
	}
	if *registedGroupIds == "" {
		fmt.Println("Please pre set groupIds to regist to master!")
		return
	}
	if *mysqlUser == "" || *mysqlPassword == "" || *mysqlHost == "" {
		fmt.Println("mysql config have to be given !")
		return
	}
	gzlog.InitGZLogger(*logFile, 50*1000*1000, 5)
	nps := spread_compute.NewProducerServer(*configPath)
	log.Printf("Start to start rpc service!")
	go nps.StartRpc()
	spread_compute.RegistProducerService(2,
		&ProducerServiceUser{MysqlHost: *mysqlHost, MysqlUser: *mysqlUser, MysqlPasswd: *mysqlPassword})
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
