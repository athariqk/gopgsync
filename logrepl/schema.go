package logrepl

import (
	"fmt"
	"log"
	"os"

	pgcdcmodels "github.com/athariqk/pgcdc-models"
	"gopkg.in/yaml.v3"
)

type Relationship struct {
	Type       string
	ForeignKey struct {
		Child  string
		Parent string
	} `yaml:"fk"`
}

type SyncMode string

const (
	SYNC_NONE SyncMode = "none"
	SYNC_ALL  SyncMode = "all"
)

type Node struct {
	Relationship Relationship
	Index        string
	PrimaryKey   string `yaml:"pk"`
	Columns      []string
	Transform    map[string]interface{}
	Children     map[string]Node
	Parent       *Node
	Sync         SyncMode
}

type Schema struct {
	Nodes map[string]Node
}

func NewSchema(filePath string) *Schema {
	bytes, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatal("Failed reading schema.yaml: ", err)
	}

	schema := &Schema{}
	err = yaml.Unmarshal(bytes, schema)
	if err != nil {
		log.Fatal("Failed parsing schema.yaml: ", err)
	}

	schema.init(schema.Nodes)

	return schema
}

func (q *Schema) GetPrimaryKey(data pgcdcmodels.DmlData) pgcdcmodels.Field {
	return data.Fields[fmt.Sprintf("%s.%s", data.TableName, q.Nodes[data.TableName].PrimaryKey)]
}

func (s *Schema) init(nodes map[string]Node) {
	for name, node := range nodes {
		if node.Sync == "" {
			node.Sync = SYNC_ALL
		}
		if node.Index == "" {
			node.Index = name
		}
		if node.PrimaryKey == "" {
			node.PrimaryKey = "id"
		}
		if node.Relationship.ForeignKey.Child == "" {
			node.Relationship.ForeignKey.Child = node.PrimaryKey
		}
		if node.Relationship.ForeignKey.Parent == "" {
			node.Relationship.ForeignKey.Parent = fmt.Sprintf("%s_id", name)
		}

		nodes[name] = node

		s.init(node.Children)
	}
}
