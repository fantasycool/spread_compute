package spread_compute

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

type Config struct {
	GroupId          string `yaml:"group_id"`
	ListenIpPort     string `yaml:"listen_ip_port"`
	HttpListenIpPort string `yaml:"listen_http_ip_port"`
	ServerRpcPort    string `yaml:"server_port"`
	ServerIp         string `yaml:"server_ip"`
	ClientRpcPort    string `yaml:"client_port"`
	ClientIp         string `yaml:"client_ip"`
	ListenIp         string `yaml:"listen_ip"`
}

func ReadConfig(path string) (*Config, error) {
	if path == "" {
		return nil, fmt.Errorf("path should not be blank")
	}
	if path[len(path)-1:] == "/" {
		return nil, fmt.Errorf("please not give a directory")
	}
	config := &Config{}
	fileData, err := ioutil.ReadFile(path)
	if err != nil {
		log.Printf("file does not exist! \n")
		return nil, err
	}
	err = yaml.Unmarshal(fileData, config)
	if err != nil {
		log.Printf("unmarshal file data failed! err:%s \n", err)
		return nil, err
	} else {
		log.Printf("config is %s \n", config)
	}
	return config, nil
}

func InitConfig(configPath string) (*Config, error) {
	if config, err := ReadConfig(configPath); err != nil {
		log.Printf("read config error! err:%s \n", err)
		return nil, err
	} else {
		return config, nil
	}
}
