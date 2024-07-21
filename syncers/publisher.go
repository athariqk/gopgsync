package syncers

import (
	"github.com/athariqk/gopgsync/logrepl"
)

type Publisher struct {
}

func (m *Publisher) Init(schema *logrepl.Schema) error {
	return nil
}
