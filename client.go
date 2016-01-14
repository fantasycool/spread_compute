package spread_compute

import (
	"fmt"
	"log"
	"sync"
)

type ProducerService interface {
	ProduceData() ([]interface{}, error)
}

type ConsumerService interface {
	ConsumeData(datas []interface{}) error
}

var (
	RegistedProducerFunctions = make(map[int64]ProducerService)
	RegistedConsumerFunctions = make(map[int64]ConsumerService)
	GroupConsumers            = make(map[int64][]ConsumerTask)
	rfLock                    = &sync.Mutex{}
)

var PS *ProducerServer
var CS *ConsumerServer
var instanceLock = &sync.Mutex{}

func RegistProducerService(groupId int64, producer ProducerService) error {
	if groupId <= 0 {
		return fmt.Errorf("illegal argument groupId")
	}
	if producer == nil {
		return fmt.Errorf("illegal argument producerService")
	}
	rfLock.Lock()
	_, exists := RegistedProducerFunctions[groupId]
	if !exists {
		RegistedProducerFunctions[groupId] = producer
	} else {
		log.Printf("groupId %d has already registed!", groupId)
	}
	rfLock.Unlock()
	return nil
}

func RegistConsumerService(groupId int64, c ConsumerService) error {
	if groupId <= 0 {
		return fmt.Errorf("illegal argument groupId")
	}
	if c == nil {
		return fmt.Errorf("illegal argument producerService")
	}
	rfLock.Lock()
	_, exists := RegistedConsumerFunctions[groupId]
	if !exists {
		RegistedConsumerFunctions[groupId] = c
	} else {
		log.Printf("groupId %d has already registed!", groupId)
	}
	rfLock.Unlock()
	return nil
}

type ProducerServiceDemo struct {
}

func (*ProducerServiceDemo) ProduceData() ([]interface{}, error) {
	data := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}
	resultData := make([]interface{}, len(data))
	for i, d := range data {
		resultData[i] = d
	}
	return resultData, nil
}

type ConsumerServiceDemo struct {
}

func (*ConsumerServiceDemo) ConsumeData(data []interface{}) error {
	for i, v := range data {
		fmt.Printf("i:%s v:%s \n", i, v)
	}
	return nil
}
