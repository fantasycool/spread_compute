package spread_compute

import (
	"log"
)

type Ready bool

type Args struct {
	GroupId int64
	Ip      string
	Type    string `producer or consumer`
}

func (c *GlobalCache) GetConsumerTasks(arg *Args, consumers *[]ConsumerTask) error {
	value, exists := c.Ccaches[arg.GroupId]
	if exists && value.IsReady {
		resultConsumers := make([]ConsumerTask, 0)
		for _, p := range value.ProducerList {
			if p.Address == arg.Ip {
				for _, c := range p.Consumers {
					resultConsumers = append(resultConsumers, *c)
				}
				break
			}
		}
		*consumers = resultConsumers
		return nil
	}
	log.Printf("Have found one not exists or is not ready;arg:%s \n ", arg)
	return nil
}

func (c *GlobalCache) IsGroupReady(arg *Args, ready *Ready) error {
	value, exists := c.Ccaches[arg.GroupId]
	if exists && value.IsReady {
		*ready = true
		return nil
	}
	return nil
}
