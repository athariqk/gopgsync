package logrepl

import (
	"github.com/athariqk/gopgsync/model"
)

type Syncer interface {
	Init(schema *Schema) error
	TryFullReplication(rows []*model.DmlData) error
	OnBegin(xid uint32) error
	OnInsert(data model.DmlData) error
	OnUpdate(data model.DmlData) error
	OnDelete(data model.DmlData) error
	OnCommit() error
}
