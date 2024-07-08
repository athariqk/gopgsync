package logrepl

import (
	"fmt"
	"reflect"
)

const (
	TRANSFORM_RENAME string = "rename"
	TRANSFORM_CONCAT string = "concat"
)

type Transformer struct {
	schema *Schema
}

func NewTransformer(schema *Schema) *Transformer {
	return &Transformer{
		schema: schema,
	}
}

func (t *Transformer) Transform(table string, node Node, data DmlData) error {
	for transform, op := range node.Transform {
		switch transform {
		case TRANSFORM_RENAME:
			t.rename(op, table, data)
		}
	}
	for name, child := range node.Children {
		t.Transform(name, child, data)
	}
	return nil
}

func (t *Transformer) rename(op interface{}, table string, data DmlData) {
	iter := reflect.ValueOf(op).MapRange()
	for iter.Next() {
		original := fmt.Sprintf("%s.%s", table, iter.Key())
		new := fmt.Sprintf("%s.%v", table, iter.Value())

		temp, ok := data.Fields[original]
		if ok {
			delete(data.Fields, original)
			data.Fields[new] = temp
		}
	}
}
