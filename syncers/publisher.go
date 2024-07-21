package syncers

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/athariqk/gopgsync/logrepl"
	"github.com/athariqk/gopgsync/model"
	"github.com/nsqio/go-nsq"
)

type Publisher struct {
	producer  *nsq.Producer
	socket    string
	currentTx *logrepl.Transaction
}

func NewPublisher(address string, port string) *Publisher {
	return &Publisher{
		socket: fmt.Sprintf("%s:%s", address, port),
	}
}

func (p *Publisher) Init(schema *logrepl.Schema) error {
	config := nsq.NewConfig()
	producer, err := nsq.NewProducer(p.socket, config)
	if err != nil {
		return err
	}

	p.producer = producer
	log.Println("Connected to NSQD:", producer.String())

	return nil
}

func (p *Publisher) TryFullReplication(rows []*model.DmlData) error {
	commands := []*model.DmlCommand{}

	for _, row := range rows {
		commands = append(commands, &model.DmlCommand{
			CmdType: model.INSERT,
			Data:    *row,
		})
	}

	json, err := json.Marshal(model.ReplicationMessage{
		TxFlag:   model.FULL_REPLICATION,
		Commands: commands,
	})
	if err != nil {
		return err
	}

	return p.producer.Publish("replication", json)
}

func (p *Publisher) OnBegin(xid uint32) error {
	p.currentTx = logrepl.NewTransaction(xid)
	return nil
}

func (p *Publisher) OnInsert(data model.DmlData) error {
	p.currentTx.DmlCommandQueue().PushBack(&model.DmlCommand{
		CmdType: model.INSERT,
		Data:    data,
	})
	return nil
}

func (p *Publisher) OnUpdate(data model.DmlData) error {
	p.currentTx.DmlCommandQueue().PushBack(&model.DmlCommand{
		CmdType: model.UPDATE,
		Data:    data,
	})
	return nil
}

func (p *Publisher) OnDelete(data model.DmlData) error {
	p.currentTx.DmlCommandQueue().PushBack(&model.DmlCommand{
		CmdType: model.DELETE,
		Data:    data,
	})
	return nil
}

func (p *Publisher) OnCommit() error {
	var batch []*model.DmlCommand
	for p.currentTx.DmlCommandQueue().Len() > 0 {
		e := p.currentTx.DmlCommandQueue().Front()
		cmd, err := model.CastToDmlCmd(e)
		if err != nil {
			return err
		}

		batch = append(batch, cmd)
		nextCmd, _ := model.CastToDmlCmd(e.Next())
		if nextCmd == nil || nextCmd.CmdType != cmd.CmdType {
			err = p.handleDmlCommands(batch)
			if err != nil {
				return err
			}
			batch = nil
		}

		p.currentTx.DmlCommandQueue().Remove(e)
	}

	p.currentTx = nil
	return nil
}

func (p *Publisher) handleDmlCommands(batch []*model.DmlCommand) error {
	json, err := json.Marshal(model.ReplicationMessage{
		TxFlag:   model.COMMIT,
		Commands: batch,
	})
	if err != nil {
		return err
	}

	log.Println("Publishing", len(batch), "commands")
	return p.producer.Publish("replication", json)
}
