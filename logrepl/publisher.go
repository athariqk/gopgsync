package logrepl

import pgcdcmodels "github.com/athariqk/pgcdc-models"

type Publisher interface {
	String() string
	Init(schema *Schema) error
	TryFullReplication(rows []*pgcdcmodels.DmlData) error
	OnBegin(xid uint32) error
	OnInsert(data pgcdcmodels.DmlData) error
	OnUpdate(data pgcdcmodels.DmlData) error
	OnDelete(data pgcdcmodels.DmlData) error
	OnCommit() error
}
