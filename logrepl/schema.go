package logrepl

import (
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

type Relationship struct {
	Key        string
	Table      string
	PrimaryKey string `yaml:"primaryKey"`
	Columns    map[string]string
	Transform  map[string]map[string]string
}

type Table struct {
	Relationships []Relationship
	Index         string
	PrimaryKey    string `yaml:"primaryKey"`
	Columns       map[string]string
	Transform     map[string]map[string]string
}

type Schema struct {
	Sync map[string]Table
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

	schema.normalizeFields()

	return schema
}

func (s *Schema) normalizeFields() {
	newTables := map[string]Table{}
	for name, table := range s.Sync {
		if table.Index == "" {
			table.Index = name
		}
		if table.PrimaryKey == "" {
			table.PrimaryKey = "id"
		}
		newRels := []Relationship{}
		for _, rel := range table.Relationships {
			if rel.PrimaryKey == "" {
				rel.PrimaryKey = "id"
			}
			newRels = append(newRels, rel)
		}
		table.Relationships = newRels
		newTables[name] = table
	}
	s.Sync = newTables
}
