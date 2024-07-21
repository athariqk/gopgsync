package model

import (
	"container/list"
	"errors"
	"strings"
)

type Field struct {
	Content     interface{}
	IsKey       bool
	DataTypeOID uint32
}

type DmlData struct {
	TableName string
	// NOTE: all field names MUST BE a fully qualified name!
	Fields map[string]Field
}

type SQLCommandType uint8

const (
	INSERT SQLCommandType = iota
	DELETE
	UPDATE
)

type DmlCommand struct {
	Data    DmlData
	CmdType SQLCommandType
}

type ReplicationMessageFlag uint8

const (
	FULL_REPLICATION ReplicationMessageFlag = iota
	COMMIT
)

type ReplicationMessage struct {
	TxFlag   ReplicationMessageFlag
	Commands []*DmlCommand
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

func CastToDmlCmd(e *list.Element) (*DmlCommand, error) {
	if e == nil {
		return nil, nil
	}
	dmlCommand, ok := e.Value.(*DmlCommand)
	if !ok {
		return nil, errors.New("incorrect casting of value to DmlCommand from queue")
	}
	return dmlCommand, nil
}
