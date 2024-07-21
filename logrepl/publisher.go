package logrepl

import gopgsyncmodels "github.com/athariqk/gopgsync-models"

type Publisher interface {
	String() string
	Init(schema *Schema) error
	TryFullReplication(rows []*gopgsyncmodels.DmlData) error
	OnBegin(xid uint32) error
	OnInsert(data gopgsyncmodels.DmlData) error
	OnUpdate(data gopgsyncmodels.DmlData) error
	OnDelete(data gopgsyncmodels.DmlData) error
	OnCommit() error
}
