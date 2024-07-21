package syncers

import (
	"encoding/json"
	"fmt"
	"log"

	gopgsyncmodel "github.com/athariqk/gopgsync-models"
	"github.com/athariqk/gopgsync/logrepl"
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

func (p *Publisher) TryFullReplication(rows []*gopgsyncmodel.DmlData) error {
	commands := []*gopgsyncmodel.DmlCommand{}

	for _, row := range rows {
		commands = append(commands, &gopgsyncmodel.DmlCommand{
			CmdType: gopgsyncmodel.INSERT,
			Data:    *row,
		})
	}

	json, err := json.Marshal(gopgsyncmodel.ReplicationMessage{
		TxFlag:   gopgsyncmodel.FULL_REPLICATION,
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

func (p *Publisher) OnInsert(data gopgsyncmodel.DmlData) error {
	p.currentTx.DmlCommandQueue().PushBack(&gopgsyncmodel.DmlCommand{
		CmdType: gopgsyncmodel.INSERT,
		Data:    data,
	})
	return nil
}

func (p *Publisher) OnUpdate(data gopgsyncmodel.DmlData) error {
	p.currentTx.DmlCommandQueue().PushBack(&gopgsyncmodel.DmlCommand{
		CmdType: gopgsyncmodel.UPDATE,
		Data:    data,
	})
	return nil
}

func (p *Publisher) OnDelete(data gopgsyncmodel.DmlData) error {
	p.currentTx.DmlCommandQueue().PushBack(&gopgsyncmodel.DmlCommand{
		CmdType: gopgsyncmodel.DELETE,
		Data:    data,
	})
	return nil
}

func (p *Publisher) OnCommit() error {
	var batch []*gopgsyncmodel.DmlCommand
	for p.currentTx.DmlCommandQueue().Len() > 0 {
		e := p.currentTx.DmlCommandQueue().Front()
		cmd, err := gopgsyncmodel.CastToDmlCmd(e)
		if err != nil {
			return err
		}

		batch = append(batch, cmd)
		nextCmd, _ := gopgsyncmodel.CastToDmlCmd(e.Next())
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

func (p *Publisher) handleDmlCommands(batch []*gopgsyncmodel.DmlCommand) error {
	json, err := json.Marshal(gopgsyncmodel.ReplicationMessage{
		TxFlag:   gopgsyncmodel.COMMIT,
		Commands: batch,
	})
	if err != nil {
		return err
	}

	log.Println("Publishing", len(batch), "commands")
	return p.producer.Publish("replication", json)
}
