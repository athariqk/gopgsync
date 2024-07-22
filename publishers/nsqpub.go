package publishers

import (
	"encoding/json"
	"fmt"

	pgcdcmodels "github.com/athariqk/pgcdc-models"
	"github.com/athariqk/pgcdc/logrepl"
	"github.com/nsqio/go-nsq"
)

type NsqPublisher struct {
	socket    string
	topic     string
	producer  *nsq.Producer
	currentTx *logrepl.Transaction
}

func NewNsqPublisher(address string, port string, topic string) *NsqPublisher {
	return &NsqPublisher{
		socket: fmt.Sprintf("%s:%s", address, port),
		topic:  topic,
	}
}

func (m *NsqPublisher) String() string {
	return "NsqPublisher"
}

func (p *NsqPublisher) Init(schema *logrepl.Schema) error {
	config := nsq.NewConfig()
	producer, err := nsq.NewProducer(p.socket, config)
	if err != nil {
		return err
	}

	p.producer = producer

	return nil
}

func (p *NsqPublisher) TryFullReplication(rows []*pgcdcmodels.Row) error {
	for _, row := range rows {
		json, err := json.Marshal(pgcdcmodels.ReplicationMessage{
			ReplicationFlag: pgcdcmodels.FULL_REPLICATION,
			Command: &pgcdcmodels.DmlCommand{
				CmdType: pgcdcmodels.INSERT,
				Data:    *row,
			},
		})
		if err != nil {
			return err
		}

		err = p.producer.Publish(p.topic, json)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *NsqPublisher) OnBegin(xid uint32) error {
	p.currentTx = logrepl.NewTransaction(xid)
	return nil
}

func (p *NsqPublisher) OnInsert(row pgcdcmodels.Row) error {
	p.currentTx.DmlCommandQueue().PushBack(&pgcdcmodels.DmlCommand{
		CmdType: pgcdcmodels.INSERT,
		Data:    row,
	})
	return nil
}

func (p *NsqPublisher) OnUpdate(row pgcdcmodels.Row) error {
	p.currentTx.DmlCommandQueue().PushBack(&pgcdcmodels.DmlCommand{
		CmdType: pgcdcmodels.UPDATE,
		Data:    row,
	})
	return nil
}

func (p *NsqPublisher) OnDelete(row pgcdcmodels.Row) error {
	p.currentTx.DmlCommandQueue().PushBack(&pgcdcmodels.DmlCommand{
		CmdType: pgcdcmodels.DELETE,
		Data:    row,
	})
	return nil
}

func (p *NsqPublisher) OnCommit() error {
	for p.currentTx.DmlCommandQueue().Len() > 0 {
		e := p.currentTx.DmlCommandQueue().Front()
		cmd, err := pgcdcmodels.CastToDmlCmd(e)
		if err != nil {
			return err
		}

		json, err := json.Marshal(pgcdcmodels.ReplicationMessage{
			ReplicationFlag: pgcdcmodels.STREAM_REPLICATION,
			Command:         cmd,
		})
		if err != nil {
			return err
		}

		err = p.producer.Publish(p.topic, json)
		if err != nil {
			return err
		}

		p.currentTx.DmlCommandQueue().Remove(e)
	}

	p.currentTx = nil
	return nil
}
