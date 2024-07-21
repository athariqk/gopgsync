package syncers

import (
	"errors"
	"fmt"
	"log"

	gopgsyncmodels "github.com/athariqk/gopgsync-models"
	"github.com/athariqk/gopgsync/logrepl"
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

func (m *MeiliSyncer) OnInsert(data gopgsyncmodels.DmlData) error {
	m.currentTx.DmlCommandQueue().PushBack(&gopgsyncmodels.DmlCommand{
		CmdType: gopgsyncmodels.INSERT,
		Data:    data,
	})
	return nil
}

func (m *MeiliSyncer) OnUpdate(data gopgsyncmodels.DmlData) error {
	m.currentTx.DmlCommandQueue().PushBack(&gopgsyncmodels.DmlCommand{
		CmdType: gopgsyncmodels.UPDATE,
		Data:    data,
	})
	return nil
}

func (m *MeiliSyncer) OnDelete(data gopgsyncmodels.DmlData) error {
	m.currentTx.DmlCommandQueue().PushBack(&gopgsyncmodels.DmlCommand{
		CmdType: gopgsyncmodels.DELETE,
		Data:    data,
	})
	return nil
}

func (m *MeiliSyncer) OnCommit() error {
	var batch []*gopgsyncmodels.DmlCommand
	for m.currentTx.DmlCommandQueue().Len() > 0 {
		e := m.currentTx.DmlCommandQueue().Front()
		cmd, err := gopgsyncmodels.CastToDmlCmd(e)
		if err != nil {
			return err
		}

		batch = append(batch, cmd)
		nextCmd, _ := gopgsyncmodels.CastToDmlCmd(e.Next())
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

func (m *MeiliSyncer) TryFullReplication(rows []*gopgsyncmodels.DmlData) error {
	if m.client == nil {
		return errors.New("meilisearch client is null")
	}

	node := m.schema.Nodes[rows[0].TableName]

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

	replicateRows := map[int64]map[string]gopgsyncmodels.Field{}
	for _, row := range rows {
		flattened := map[string]gopgsyncmodels.Field{}
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
		err = m.OnInsert(gopgsyncmodels.DmlData{
			TableName: rows[0].TableName,
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

func (m *MeiliSyncer) handleDmlCommands(batch []*gopgsyncmodels.DmlCommand) error {
	if m.client == nil {
		log.Fatal("[MeiliSyncer] Meilisearch client is null")
	}

	if len(batch) <= 0 {
		return nil
	}

	table := m.schema.Nodes[batch[0].Data.TableName]

	switch batch[0].CmdType {
	case gopgsyncmodels.INSERT:
		var documents []*map[string]interface{}
		for _, cmd := range batch {
			columns := gopgsyncmodels.Flatten(cmd.Data.Fields, false)
			documents = append(documents, &columns)
		}

		resps, err := m.client.Index(table.Index).AddDocumentsInBatches(documents, 50, table.PrimaryKey)
		if err != nil {
			return err
		}
		for _, resp := range resps {
			log.Printf("[MeiliSyncer] Batched Task UID: %v of Type: %s status: %s", resp.TaskUID, resp.Type, resp.Status)
		}
	case gopgsyncmodels.UPDATE:
		var documents []*map[string]interface{}
		for _, cmd := range batch {
			columns := gopgsyncmodels.Flatten(cmd.Data.Fields, false)
			documents = append(documents, &columns)
		}

		resps, err := m.client.Index(table.Index).UpdateDocumentsInBatches(documents, 50, table.PrimaryKey)
		if err != nil {
			return err
		}
		for _, resp := range resps {
			log.Printf("[MeiliSyncer] Batched Task UID: %v of Type: %s status: %s", resp.TaskUID, resp.Type, resp.Status)
		}
	case gopgsyncmodels.DELETE:
		var refNumbers []string
		for _, x := range batch {
			keys := gopgsyncmodels.Flatten(x.Data.Fields, true)
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
