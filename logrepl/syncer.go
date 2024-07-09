package logrepl

import "strings"

type Field struct {
	Content     interface{}
	IsKey       bool
	DataTypeOID uint32
}

// Flattens field map into basic column names (<table>.<field>) with plain field values
func Flatten(columns map[string]Field, onlyKey bool) map[string]interface{} {
	row := map[string]interface{}{}
	for name, field := range columns {
		if onlyKey && !field.IsKey {
			continue
		}
		splits := strings.Split(name, ".")
		if len(splits) == 2 {
			name = splits[1]
		}
		row[name] = field.Content
	}
	return row
}

type Syncer interface {
	OnInit(schema *Schema) error
	TryFullReplication(rows []*DmlData) error
	OnBegin(xid uint32) error
	OnInsert(data DmlData) error
	OnDelete(data DmlData) error
	OnCommit() error
}
