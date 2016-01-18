package spread_compute

import (
	"fmt"
	"log"
)

func ShuffleData(groupId int64, datas []interface{}) error {
	log.Printf("Starting to shuffle data \n")
	if PS == nil {
		log.Printf("producerServer may not be inited!Please init firstly!\n")
		return fmt.Errorf("producer server may not be inited,groupId:%d!", groupId)
	}
	if GroupConsumers[groupId] == nil {
		log.Printf("consumer list is not ready")
		return fmt.Errorf("consumer list is not ready!groupId:%d \n", groupId)
	}
	consumerList := GroupConsumers[groupId]
	if len(consumerList) == 0 {
		log.Printf("no consumers to execute command! \n")
		return fmt.Errorf("no consumers to execute command!groupId:%d \n", groupId)
	}
	for index, c := range datas {
		mod := index % len(consumerList)
		log.Printf("data should go to consumer %d \n", mod)
		if consumerList[mod].Data == nil {
			consumerList[mod].Data = make([]interface{}, 0)
		}
		consumerList[mod].Data = append(consumerList[mod].Data, c)
	}
	err := SendToConsumerExec(consumerList, groupId)
	if err != nil {
		log.Printf("Send to consumers exec failed! err:%s \n", err)
		return err
	}
	return nil
}

func ConsumeData(groupId int64, datas []interface{}) error {
	if groupId <= 0 {
		return fmt.Errorf("invalid groupId %d \n", groupId)
	}
	if CS == nil {
		log.Printf("ConsumerServer may not be inited!Please init first! \n")
		return fmt.Errorf("ConsumerServer may not be inited!Please init first! \n")
	}
	rfLock.Lock()
	cs := RegistedConsumerFunctions[groupId]
	rfLock.Unlock()
	log.Printf("Start to consume data!groupId:%d \n", groupId)
	err := cs.ConsumeData(datas)
	if err != nil {
		log.Print("consume data failed,err:%s \n", groupId)
		return fmt.Errorf("Consum data error!groupId:%d \n", groupId)
	}
	return nil
}
