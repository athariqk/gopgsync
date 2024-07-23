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

// Notes on the full replication protocol:
//   - FULL_REPLICATION_NEW_ROWS will only contain namespace and relation name without row fields values
//   - FULL_REPLICATION_NEW_ROWS will be sent every rowset change (iterating over another table rows)
//   - FULL_REPLICATION_FINISHED will be sent when all rowsets have been published
func (p *NsqPublisher) FullyReplicateTable(rows []*pgcdcmodels.Row, totalTables int) error {
	result, err := json.Marshal(pgcdcmodels.ReplicationMessage{
		ReplicationFlag: pgcdcmodels.FULL_REPLICATION_NEW_ROWS,
		Command: &pgcdcmodels.DmlCommand{Data: pgcdcmodels.Row{
			Namespace: rows[0].Namespace,
			RelName:   rows[0].RelName,
		}},
		Total: len(rows),
	})
	if err != nil {
		return err
	}
	err = p.producer.Publish(p.topic, result)
	if err != nil {
		return err
	}

	for _, row := range rows {
		result, err = json.Marshal(pgcdcmodels.ReplicationMessage{
			ReplicationFlag: pgcdcmodels.FULL_REPLICATION_PROGRESS,
			Command: &pgcdcmodels.DmlCommand{
				CmdType: pgcdcmodels.INSERT,
				Data:    *row,
			},
			Total: len(rows),
		})
		if err != nil {
			return err
		}

		err = p.producer.Publish(p.topic, result)
		if err != nil {
			return err
		}
	}

	// "End byte"
	result, err = json.Marshal(pgcdcmodels.ReplicationMessage{
		ReplicationFlag: pgcdcmodels.FULL_REPLICATION_FINISHED,
		Command: &pgcdcmodels.DmlCommand{Data: pgcdcmodels.Row{
			Namespace: rows[0].Namespace,
			RelName:   rows[0].RelName,
		}},
		Total: totalTables,
	})
	if err != nil {
		return err
	}
	err = p.producer.Publish(p.topic, result)
	if err != nil {
		return err
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
