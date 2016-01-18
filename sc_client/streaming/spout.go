package streaming

import (
	"fmt"
	"log"
)

type Bucket struct {
	msgs []Message
}

type Message struct {
	value interface{}
}

//Data Father Interface
type SnowMan interface {
	InitVar() error
	Next() (Message, error)
	Stop() error
	Callback(m Message) error
}

type SnowManScheduler struct {
	SnowMans []*SnowMan
}

type SnowManStatus struct {
	SnowMan *SnowMan
	Status  int
}

func (sn *SnowManScheduler) Regist(s *SnowMan) error {
	if sn.SnowMans == nil {
		return fmt.Errorf("no snow man is registed! \n")
	}
	for _, sn := range sn.SnowMans {
		if sn == s {
			log.Printf("This snow man has already registed!")
			return nil
		}
	}
	return nil
}

func (sn *SnowManScheduler) Start() error {
	return nil
}

func (sn *SnowManScheduler) Stop() error {
	return nil
}
