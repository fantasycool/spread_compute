package spread_compute

import (
	"log"
	"time"
)

type GlobalCache struct {
	Ccaches map[int64]*ConnectorCache
}

type ConnectorCache struct {
	Id           int64
	ProducerList []*ProducerTask
	ConsumerList []*ConsumerTask
	StartTime    time.Time `json:"-"`
	IsFinished   bool
	IsReady      bool
}

func QueryOkConnector(m *MasterServer) {
	for {
		CacheMutex.Lock()
		for groupId, v := range globalCache.Ccaches {
			interval := time.Since(v.StartTime)
			if interval.Seconds() > float64(60) &&
				len(globalCache.Ccaches[groupId].ProducerList) > 0 &&
				len(globalCache.Ccaches[groupId].ConsumerList) > 0 {
				//	log.Printf("Bigger than 20 seconds, Do allocate \n")
				DoAllocate(groupId)
			}
		}
		CacheMutex.Unlock()
		time.Sleep(1000 * time.Millisecond)
	}
}

func DoAllocate(groupId int64) {
	connectorCache := globalCache.Ccaches[groupId]
	log.Printf("connectorCache ready status is %b \n", connectorCache.IsReady)
	if !connectorCache.IsReady {
		for index, consumer := range connectorCache.ConsumerList {
			if len(connectorCache.ProducerList) == 0 {
				continue
			}
			p := connectorCache.ProducerList[index%len(connectorCache.ProducerList)]
			if p.Consumers == nil {
				p.Consumers = []*ConsumerTask{consumer}
				continue
			}
			p.Consumers = append(p.Consumers, consumer)
		}
		log.Printf("groupId is ready!length is %d and consumers is %s \n",
			len(connectorCache.ConsumerList), connectorCache.ConsumerList)
		connectorCache.IsReady = true
	}
}
