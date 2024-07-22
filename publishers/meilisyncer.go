package publishers

import (
	"errors"
	"fmt"
	"log"

	pgcdcmodels "github.com/athariqk/pgcdc-models"
	"github.com/athariqk/pgcdc/logrepl"
	"github.com/meilisearch/meilisearch-go"
)

type MeiliSyncer struct {
	client    *meilisearch.Client
	schema    *logrepl.Schema
	currentTx *logrepl.Transaction
}

func NewMeiliSyncer(meilisearchConfig meilisearch.ClientConfig) *MeiliSyncer {
	return &MeiliSyncer{
		client: meilisearch.NewClient(meilisearchConfig),
	}
}

func (m *MeiliSyncer) String() string {
	return "Meilisyncer"
}

func (m *MeiliSyncer) Init(schema *logrepl.Schema) error {
	m.schema = schema

	resp, err := m.client.GetVersion()
	if err != nil {
		return err
	}

	log.Println("[MeiliSyncer] Connected to MeiliSearch version:", resp.PkgVersion)
	return nil
}

func (m *MeiliSyncer) OnBegin(xid uint32) error {
	m.currentTx = logrepl.NewTransaction(xid)
	return nil
}

func (m *MeiliSyncer) OnInsert(data pgcdcmodels.Row) error {
	node := m.schema.Nodes[data.RelName]
	if node.Sync != logrepl.SYNC_ALL {
		return nil
	}
	m.currentTx.DmlCommandQueue().PushBack(&pgcdcmodels.DmlCommand{
		CmdType: pgcdcmodels.INSERT,
		Data:    data,
	})
	return nil
}

func (m *MeiliSyncer) OnUpdate(data pgcdcmodels.Row) error {
	node := m.schema.Nodes[data.RelName]
	if node.Sync != logrepl.SYNC_ALL {
		return nil
	}
	m.currentTx.DmlCommandQueue().PushBack(&pgcdcmodels.DmlCommand{
		CmdType: pgcdcmodels.UPDATE,
		Data:    data,
	})
	return nil
}

func (m *MeiliSyncer) OnDelete(data pgcdcmodels.Row) error {
	node := m.schema.Nodes[data.RelName]
	if node.Sync != logrepl.SYNC_ALL {
		return nil
	}
	m.currentTx.DmlCommandQueue().PushBack(&pgcdcmodels.DmlCommand{
		CmdType: pgcdcmodels.DELETE,
		Data:    data,
	})
	return nil
}

func (m *MeiliSyncer) OnCommit() error {
	var batch []*pgcdcmodels.DmlCommand
	for m.currentTx.DmlCommandQueue().Len() > 0 {
		e := m.currentTx.DmlCommandQueue().Front()
		cmd, err := pgcdcmodels.CastToDmlCmd(e)
		if err != nil {
			return err
		}

		batch = append(batch, cmd)
		nextCmd, _ := pgcdcmodels.CastToDmlCmd(e.Next())
		if nextCmd == nil || nextCmd.CmdType != cmd.CmdType {
			err = m.handleDmlCommands(batch)
			if err != nil {
				return err
			}
			batch = nil
		}

		m.currentTx.DmlCommandQueue().Remove(e)
	}

	m.currentTx = nil
	return nil
}

func (m *MeiliSyncer) TryFullReplication(rows []*pgcdcmodels.Row) error {
	node := m.schema.Nodes[rows[0].RelName]
	if node.Sync != logrepl.SYNC_ALL {
		return nil
	}

	if m.client == nil {
		return errors.New("meilisearch client is null")
	}

	taskInfo, err := m.client.CreateIndex(&meilisearch.IndexConfig{
		Uid:        node.Index,
		PrimaryKey: node.PrimaryKey,
	})
	if err != nil {
		return err
	}

	_, err = m.client.WaitForTask(taskInfo.TaskUID)
	if err != nil {
		return err
	}

	replicateRows := map[int64]map[string]pgcdcmodels.Field{}
	for _, row := range rows {
		flattened := map[string]pgcdcmodels.Field{}
		for name, field := range row.Fields {
			flattened[name] = field
		}
		pk := m.schema.GetPrimaryKey(*row).Content.(int64)
		replicateRows[pk] = flattened
	}

	lastOffset := int64(0)
	for {
		result := &meilisearch.DocumentsResult{}
		err = m.client.Index(node.Index).GetDocuments(&meilisearch.DocumentsQuery{
			Offset: lastOffset,
			Limit:  100,
		}, result)
		if err != nil {
			return err
		}

		for _, document := range result.Results {
			id := int64(document[node.PrimaryKey].(float64))
			_, ok := replicateRows[id]
			if ok {
				delete(replicateRows, id)
			}
		}

		lastOffset += int64(len(result.Results))
		if lastOffset == result.Total {
			break
		}
	}

	log.Printf("[MeiliSyncer] got %v documents in index `%s`, will add %v more from source",
		lastOffset,
		node.Index,
		len(replicateRows))

	err = m.OnBegin(0)
	if err != nil {
		return err
	}

	// TODO: batching and concurrency
	for _, replicateRow := range replicateRows {
		err = m.OnInsert(pgcdcmodels.Row{
			Namespace: rows[0].Namespace,
			RelName:   rows[0].RelName,
			Fields:    replicateRow,
		})
		if err != nil {
			return err
		}
	}

	err = m.OnCommit()
	if err != nil {
		return err
	}

	return nil
}

func (m *MeiliSyncer) handleDmlCommands(batch []*pgcdcmodels.DmlCommand) error {
	if m.client == nil {
		log.Fatal("[MeiliSyncer] Meilisearch client is null")
	}

	if len(batch) <= 0 {
		return nil
	}

	table := m.schema.Nodes[batch[0].Data.RelName]

	switch batch[0].CmdType {
	case pgcdcmodels.INSERT:
		var documents []*map[string]interface{}
		for _, cmd := range batch {
			columns := pgcdcmodels.Flatten(cmd.Data.Fields, false)
			documents = append(documents, &columns)
		}

		resps, err := m.client.Index(table.Index).AddDocumentsInBatches(documents, 50, table.PrimaryKey)
		if err != nil {
			return err
		}
		for _, resp := range resps {
			log.Printf("[MeiliSyncer] Batched Task UID: %v of Type: %s status: %s", resp.TaskUID, resp.Type, resp.Status)
		}
	case pgcdcmodels.UPDATE:
		var documents []*map[string]interface{}
		for _, cmd := range batch {
			columns := pgcdcmodels.Flatten(cmd.Data.Fields, false)
			documents = append(documents, &columns)
		}

		resps, err := m.client.Index(table.Index).UpdateDocumentsInBatches(documents, 50, table.PrimaryKey)
		if err != nil {
			return err
		}
		for _, resp := range resps {
			log.Printf("[MeiliSyncer] Batched Task UID: %v of Type: %s status: %s", resp.TaskUID, resp.Type, resp.Status)
		}
	case pgcdcmodels.DELETE:
		var refNumbers []string
		for _, x := range batch {
			keys := pgcdcmodels.Flatten(x.Data.Fields, true)
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
