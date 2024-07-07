package logrepl

import (
	"context"
	"log"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

type QueryBuilder struct {
	Schema  *Schema
	pgConn  *pgconn.PgConn
	typeMap *pgtype.Map
}

func NewQueryBuilder(pgConnectionString string, schema *Schema) *QueryBuilder {
	conn, err := pgconn.Connect(context.Background(), pgConnectionString)
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("[QueryBuilder] Connected to PostgreSQL PID:", conn.PID())

	return &QueryBuilder{
		Schema:  schema,
		pgConn:  conn,
		typeMap: pgtype.NewMap(),
	}
}

func (q *QueryBuilder) ResolveRelationships(data DmlData) error {
	query := q.SelectAndJoin(data.TableName)

	results, err := q.pgConn.Exec(context.Background(), query).ReadAll()
	if err != nil {
		return err
	}

	for _, resultRow := range results[0].Rows {
		for idx, fieldDesc := range results[0].FieldDescriptions {
			fieldData := resultRow[idx]
			decoded, err := q.decode(fieldData, fieldDesc.DataTypeOID, fieldDesc.Format)
			if err != nil {
				return err
			}
			(*data.Values)[fieldDesc.Name] = Column{
				Data: decoded,
			}
		}
	}

	return nil
}

func (q *QueryBuilder) SelectAndJoin(tableName string) string {
	table := q.Schema.Sync[tableName]

	query := strings.Builder{}

	query.WriteString("SELECT ")
	idx := 0
	for _, detail := range table.Relationships {
		for column := range detail.Columns {
			if idx > 0 {
				query.WriteString(", ")
			}
			query.WriteString(detail.Table)
			query.WriteString(".")
			query.WriteString(column)
			for transformType, transforms := range detail.Transform {
				switch transformType {
				case "rename":
					to := transforms[column]
					query.WriteString(" AS ")
					query.WriteString(to)
				}
			}
			idx++
		}
	}

	query.WriteString(" FROM ")
	query.WriteString(tableName)

	for _, rel := range table.Relationships {
		query.WriteString(" JOIN ")
		query.WriteString(rel.Table)
		query.WriteString(" ON ")
		query.WriteString(tableName)
		query.WriteString(".")
		query.WriteString(rel.Key)
		query.WriteString("=")
		query.WriteString(rel.Table)
		query.WriteString(".")
		query.WriteString(rel.PrimaryKey)
	}

	query.WriteString(";")

	return query.String()
}

func (q *QueryBuilder) decode(data []byte, dataType uint32, format int16) (interface{}, error) {
	if dt, ok := q.typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(q.typeMap, dataType, format, data)
	}
	return data, nil
}
