package publishers

import (
	"encoding/json"
	"fmt"
	"log"

	pgcdcmodels "github.com/athariqk/pgcdc-models"
	"github.com/athariqk/pgcdc/logrepl"
	"github.com/nsqio/go-nsq"
)

type NsqPublisher struct {
	producer  *nsq.Producer
	socket    string
	currentTx *logrepl.Transaction
}

func NewNsqPublisher(address string, port string) *NsqPublisher {
	return &NsqPublisher{
		socket: fmt.Sprintf("%s:%s", address, port),
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
	commands := []*pgcdcmodels.DmlCommand{}

	for _, row := range rows {
		commands = append(commands, &pgcdcmodels.DmlCommand{
			CmdType: pgcdcmodels.INSERT,
			Data:    *row,
		})
	}

	json, err := json.Marshal(pgcdcmodels.ReplicationMessage{
		TxFlag:   pgcdcmodels.FULL_REPLICATION,
		Commands: commands,
	})
	if err != nil {
		return err
	}

	return p.producer.Publish("replication", json)
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
	var batch []*pgcdcmodels.DmlCommand
	for p.currentTx.DmlCommandQueue().Len() > 0 {
		e := p.currentTx.DmlCommandQueue().Front()
		cmd, err := pgcdcmodels.CastToDmlCmd(e)
		if err != nil {
			return err
		}

		batch = append(batch, cmd)
		nextCmd, _ := pgcdcmodels.CastToDmlCmd(e.Next())
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

func (p *NsqPublisher) handleDmlCommands(batch []*pgcdcmodels.DmlCommand) error {
	json, err := json.Marshal(pgcdcmodels.ReplicationMessage{
		TxFlag:   pgcdcmodels.COMMIT,
		Commands: batch,
	})
	if err != nil {
		return err
	}

	log.Println("Publishing", len(batch), "commands")
	return p.producer.Publish("replication", json)
}
