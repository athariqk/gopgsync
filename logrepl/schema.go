package logrepl

import (
	"fmt"
	"log"
	"os"
	"strings"

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

type CaptureMode string

const (
	CAPTURE_NONE CaptureMode = "none"
	CAPTURE_ALL  CaptureMode = "all"
)

type Node struct {
	Relationship Relationship
	Namespace    string
	PrimaryKey   string `yaml:"pk"`
	Columns      []string
	Transform    map[string]interface{}
	Children     map[string]Node
	Parent       *Node
	Capture      CaptureMode
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

func (q *Schema) GetPrimaryKey(data pgcdcmodels.Row) pgcdcmodels.Field {
	return data.Fields[fmt.Sprintf("%s.%s.%s", data.Namespace, data.RelName, q.Nodes[data.RelName].PrimaryKey)]
}

func (s *Schema) init(nodes map[string]Node) {
	for name, node := range nodes {
		if node.Capture == "" {
			node.Capture = CAPTURE_ALL
		}
		if node.Namespace == "" {
			node.Namespace = "public"
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

		for i, column := range node.Columns {
			if len(strings.Split(column, ".")) < 3 {
				node.Columns[i] = fmt.Sprintf("%s.%s.%s", node.Namespace, name, column)
			}
		}

		nodes[name] = node

		s.init(node.Children)
	}
}
