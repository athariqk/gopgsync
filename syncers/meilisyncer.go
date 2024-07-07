package syncers

import (
	"athariqk/gopgsync/logrepl"
	"fmt"
	"log"

	"github.com/jackc/pglogrepl"
	"github.com/meilisearch/meilisearch-go"
)

type MeiliSyncer struct {
	client       *meilisearch.Client
	queryBuilder *logrepl.QueryBuilder
	currentTx    *logrepl.Transaction
}

func NewMeiliSyncer(meilisearchConfig meilisearch.ClientConfig) *MeiliSyncer {
	return &MeiliSyncer{
		client: meilisearch.NewClient(meilisearchConfig),
	}
}

func (m *MeiliSyncer) OnInit(queryBuilder *logrepl.QueryBuilder) error {
	m.queryBuilder = queryBuilder

	resp, err := m.client.GetVersion()
	if err != nil {
		return err
	}

	log.Println("[MeiliSyncer] Connected to MeiliSearch version:", resp.PkgVersion)

	return nil
}

func (m *MeiliSyncer) OnBegin(msg *pglogrepl.BeginMessage) error {
	m.currentTx = logrepl.NewTransaction(msg.Xid)
	return nil
}

func (m *MeiliSyncer) OnInsert(data logrepl.DmlData) error {
	err := m.queryBuilder.ResolveRelationships(data)
	if err != nil {
		return err
	}

	m.currentTx.DmlCommandQueue().PushBack(&logrepl.DmlCommand{
		Data:    data,
		CmdType: logrepl.INSERT,
	})

	return nil
}

func (m *MeiliSyncer) OnDelete(data logrepl.DmlData) error {
	m.currentTx.DmlCommandQueue().PushBack(&logrepl.DmlCommand{
		Data:    data,
		CmdType: logrepl.DELETE,
	})
	return nil
}

func (m *MeiliSyncer) OnCommit(msg *pglogrepl.CommitMessage) error {
	var batch []*logrepl.DmlCommand
	for m.currentTx.DmlCommandQueue().Len() > 0 {
		e := m.currentTx.DmlCommandQueue().Front()
		cmd, err := logrepl.CastToDmlCmd(e)
		if err != nil {
			return err
		}

		batch = append(batch, cmd)
		nextCmd, _ := logrepl.CastToDmlCmd(e.Next())
		if nextCmd != nil && nextCmd.CmdType == cmd.CmdType {
			batch = append(batch, cmd)
		} else {
			err = m.handleDmlCommands(batch)
			if err != nil {
				return err
			}
			batch = nil
		}

		m.currentTx.DmlCommandQueue().Remove(e)
	}

	return nil
}

func (m *MeiliSyncer) handleDmlCommands(batch []*logrepl.DmlCommand) error {
	if m.client == nil {
		log.Fatal("[MeiliSyncer] Meilisearch client is null")
	}

	if len(batch) <= 0 {
		return nil
	}

	table := m.queryBuilder.Schema.Sync[batch[0].Data.TableName]

	switch batch[0].CmdType {
	case logrepl.INSERT:
		var documents []*map[string]interface{}
		for _, x := range batch {
			documents = append(documents, x.Data.Values.Flatten())
		}

		keyNames := *batch[0].Data.Values.GetKeyNames()
		resps, err := m.client.Index(table.Index).AddDocumentsInBatches(documents, 5, keyNames...)
		if err != nil {
			return err
		}
		for _, resp := range resps {
			log.Printf("[MeiliSyncer] Batched Task UID: %v of Type: %s status: %s", resp.TaskUID, resp.Type, resp.Status)
		}
	case logrepl.DELETE:
		var refNumbers []string
		for _, x := range batch {
			keys := *x.Data.Values.GetKeys()
			refNumbers = append(refNumbers, fmt.Sprintf("%v", keys[table.PrimaryKey]))
		}

		resp, err := m.client.Index(table.Index).DeleteDocuments(refNumbers)
		if err != nil {
			return err
		}
		log.Printf("[MeiliSyncer] Task UID: %v of Type: %s status: %s", resp.TaskUID, resp.Type, resp.Status)
	}

	return nil
}
