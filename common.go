package spread_compute

import (
	"fmt"
	"log"
)

type Call int

type CommonFunctionCallArgs struct {
	Datas    []interface{}
	GroupId  int64
	CallType string
}

type Status bool

func (c *Call) CallFunction(arg *CommonFunctionCallArgs, status *Status) error {
	switch arg.CallType {
	case "producer":
		rfLock.Lock()
		f, exists := RegistedProducerFunctions[arg.GroupId]
		rfLock.Unlock()
		if !exists {
			*status = false
			return fmt.Errorf("groupId:%d Producer function does not exists! \n", arg.GroupId)
		}
		if datas, err := f.ProduceData(); err != nil {
			*status = false
			log.Printf("Call groupId %d function failed! \n", arg.GroupId)
			return err
		} else {
			log.Printf("Start to shuffle producer data!data's length is %d \n", len(datas))
			err := ShuffleData(arg.GroupId, datas)
			if err != nil {
				*status = false
				log.Printf("Call ShuffleData failed !err:%s \n", err)
				return err
			}
		}
	case "consumer":
		rfLock.Lock()
		_, exists := RegistedConsumerFunctions[arg.GroupId]
		rfLock.Unlock()
		if exists {
			err := ConsumeData(arg.GroupId, arg.Datas)
			if err != nil {
				*status = false
				return fmt.Errorf("consume data failed ! err:%s \n", err)
			}
		} else {
			*status = false
			log.Printf("Function does not exist! \n")
			return fmt.Errorf("Function does not exist! \n")
		}
	default:
		*status = false
		return fmt.Errorf("illegal args common function call args!")
	}
	*status = true
	return nil
}
