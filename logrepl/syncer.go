package logrepl

import "github.com/jackc/pglogrepl"

type ColumnMap map[string]Column

func (c *ColumnMap) GetKeyNames() *[]string {
	result := []string{}
	for name, value := range *c {
		if value.IsKey {
			result = append(result, name)
		}
	}
	return &result
}

func (c *ColumnMap) GetKeys() *map[string]interface{} {
	result := map[string]interface{}{}
	for name, value := range *c {
		if value.IsKey {
			result[name] = value.Data
		}
	}
	return &result
}

func (c *ColumnMap) Flatten() *map[string]interface{} {
	result := map[string]interface{}{}
	for name, value := range *c {
		result[name] = value.Data
	}
	return &result
}

type Column struct {
	Data  interface{}
	IsKey bool
}

type Syncer interface {
	OnInit(queryBuilder *QueryBuilder) error
	OnBegin(msg *pglogrepl.BeginMessage) error
	OnInsert(data DmlData) error
	OnDelete(data DmlData) error
	OnCommit(msg *pglogrepl.CommitMessage) error
}
