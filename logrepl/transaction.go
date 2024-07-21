package logrepl

import (
	"container/list"
)

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
