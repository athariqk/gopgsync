package logrepl

import (
	"container/list"
	"errors"
)

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

type Transaction struct {
	Xid             uint32
	dmlCommandQueue *list.List
}

func NewTransaction(xid uint32) *Transaction {
	return &Transaction{
		Xid:             xid,
		dmlCommandQueue: list.New(),
	}
}

func (t *Transaction) DmlCommandQueue() *list.List {
	return t.dmlCommandQueue
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
