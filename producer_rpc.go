package spread_compute

import (
	"fmt"
	"log"
	"net/rpc/jsonrpc"
	"runtime"
)

type PMSync int

type SyncConsumerArgs struct {
	GroupId   int64
	Consumers []ConsumerTask
}

func (p *PMSync) SyncConsumer(arg *SyncConsumerArgs, status *Status) error {
	if arg == nil {
		*status = false
		return fmt.Errorf("sync consumer args cannot be nil ! \n")
	}
	if arg.GroupId <= 0 {
		*status = false
		return fmt.Errorf("sync consumer groupId is not valid.groupId:%d \n", arg.GroupId)
	}
	if len(arg.Consumers) == 0 {
		*status = false
		return fmt.Errorf("arg consumers length is zero! \n")
	}
	instanceLock.Lock()
	defer instanceLock.Unlock()
	log.Printf("Producer has got consumers tasks %s \n", arg.Consumers)
	GroupConsumers[arg.GroupId] = arg.Consumers
	*status = true
	return nil
}

func SendToConsumerExec(consumerList []ConsumerTask, groupId int64) error {
	channelLimit := make(chan bool, len(consumerList))
	successTasks := make(chan int, len(consumerList))
	failedTasks := make([]int, 0)
	queue := make(chan int, 1)
	log.Printf("Start to send to consumer to execute! consumerList:%s\n", consumerList)
	for i, v := range consumerList {
		//TODO Exception Retry
		go func(consumerTask ConsumerTask, i int) error {
			defer func() { channelLimit <- true }()
			log.Printf("Start to send consumer %s, port:%d to execute \n", consumerTask.Address, consumerTask.RpcPort)
			err := DoCallFunction(consumerTask, queue, i, successTasks)
			if err != nil {
				return err
			}
			return nil
		}(v, i)

		go func() {
			for v := range queue {
				failedTasks = append(failedTasks, v)
			}
		}()
	}

	for i := 0; i < len(consumerList); i++ {
		<-channelLimit
		log.Printf("All consumer jobs have done!failedTasks number is %d \n", len(failedTasks))
		if len(failedTasks) > 0 {
			log.Printf("there is failed tasks we need to to retry operation, and select a last time success server! \n")

		}
		select {
		case s := <-successTasks:
			for _, f := range failedTasks {
				args := &CommonFunctionCallArgs{
					Datas:    consumerList[f].Data,
					GroupId:  consumerList[f].GroupId,
					CallType: "consumer",
				}
				client, err := jsonrpc.Dial("tcp", fmt.Sprintf("%s:%d", consumerList[s].Address, consumerList[s].RpcPort))
				if err != nil {
					log.Printf("dial failed!%s \n", consumerList[f])
					continue
				}
				var result *Status = new(Status)
				err = client.Call("Call.CallFunction", args, result)
				if err != nil {
					log.Printf("dial failed!%s \n", consumerList[f])
					continue
				}
			}
		default:
			log.Printf("Sorry, we don't have success tasks!")
			break
		}

	}
	return nil
}

func DoCallFunction(consumerTask ConsumerTask, queue chan int, i int, success chan int) error {
	client, err := jsonrpc.Dial("tcp", fmt.Sprintf("%s:%d", consumerTask.Address, consumerTask.RpcPort))
	if err != nil {
		log.Printf("Dial client failed! err:%s \n", err)
		runtime.Gosched()
		queue <- i
		return err
	}
	args := &CommonFunctionCallArgs{
		Datas:    consumerTask.Data,
		GroupId:  consumerTask.GroupId,
		CallType: "consumer",
	}
	var result *Status = new(Status)
	err = client.Call("Call.CallFunction", args, result)
	if err != nil {
		log.Printf("send data to consumer execute failed! err:%s\n", err)
		runtime.Gosched()
		queue <- i
		return err
	}
	success <- i
	return nil
}
