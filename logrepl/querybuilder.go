package logrepl

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	pgcdcmodels "github.com/athariqk/pgcdc-models"
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

func (q *QueryBuilder) GetRows(
	context context.Context,
	namespace string,
	table string,
	columns ...string,
) ([]*pgcdcmodels.Row, error) {
	query, _ := q.Select(table, columns...)
	result, err := q.pgConn.Exec(context, query).ReadAll()
	if err != nil {
		return nil, err
	}

	rows := []*pgcdcmodels.Row{}
	for _, row := range result[0].Rows {
		fields := map[string]pgcdcmodels.Field{}
		for fieldIdx, field := range row {
			fieldDesc := result[0].FieldDescriptions[fieldIdx]
			decoded, err := q.decode(field, fieldDesc.DataTypeOID, fieldDesc.Format)
			if err != nil {
				return nil, err
			}
			fields[fmt.Sprintf("%s.%s.%s", namespace, table, fieldDesc.Name)] = pgcdcmodels.Field{
				Content:     decoded,
				IsKey:       fieldDesc.Name == q.Schema.Nodes[table].PrimaryKey,
				DataTypeOID: fieldDesc.DataTypeOID,
			}
		}
		rows = append(rows, &pgcdcmodels.Row{
			Namespace: namespace,
			RelName:   table,
			Fields:    fields,
		})
	}

	return rows, nil
}

func (q *QueryBuilder) ResolveRelationships(
	context context.Context,
	data pgcdcmodels.Row,
) error {
	node := q.Schema.Nodes[data.RelName]
	pk := q.Schema.GetPrimaryKey(data).Content.(int64)
	query := q.SelectRowIncludeReferences(data.RelName, pk, node.Columns...)

	results, err := q.pgConn.Exec(context, query).ReadAll()
	if err != nil {
		return err
	}

	if len(results) <= 0 || len(results[0].Rows) <= 0 {
		return nil
	}

	row := results[0].Rows[0] // only one row is returned
	for idx, fieldDesc := range results[0].FieldDescriptions {
		fieldData := row[idx]
		decoded, err := q.decode(fieldData, fieldDesc.DataTypeOID, fieldDesc.Format)
		if err != nil {
			return err
		}

		// shouldn't be a problem performance-wise since there'll be only one row...
		getTableName, err := q.pgConn.Exec(context, fmt.Sprintf(
			"select relname from pg_class where oid=%v", fieldDesc.TableOID)).ReadAll()
		if err != nil {
			return err
		}

		fieldTableName, err := q.decode(
			getTableName[0].Rows[0][0],
			getTableName[0].FieldDescriptions[0].DataTypeOID,
			getTableName[0].FieldDescriptions[0].Format)
		if err != nil {
			return err
		}

		fieldName := fmt.Sprintf("%s.%s.%s", data.Namespace, fieldTableName, fieldDesc.Name)
		data.Fields[fieldName] = pgcdcmodels.Field{
			Content:     decoded,
			IsKey:       data.Fields[fieldDesc.Name].IsKey,
			DataTypeOID: fieldDesc.DataTypeOID,
		}
	}

	return nil
}

func (q *QueryBuilder) Select(table string, columns ...string) (string, bool) {
	node := q.Schema.Nodes[table]

	query := strings.Builder{}

	if len(columns) <= 0 {
		log.Fatalln("No column specified")
	}

	hasQi := false

	query.WriteString("SELECT ")
	for idx, column := range columns {
		if idx > 0 {
			query.WriteString(", ")
		}

		columnName := column
		splits := strings.Split(column, ".")
		if len(splits) == 3 {
			columnName = splits[2]
		}

		_, isQi := node.Privacy.QuasiIdentifiers[columnName]
		if node.Privacy.Enabled && isQi {
			if !hasQi {
				hasQi = true
			}
			query.WriteString(fmt.Sprintf("%s_anon.%s", table, columnName))
		} else {
			query.WriteString(column)
		}
	}
	query.WriteString(" FROM ")
	query.WriteString(table)

	if node.Privacy.Enabled && hasQi {
		query.WriteString(" JOIN ")
		query.WriteString(fmt.Sprintf("%s.%s %s_anon", node.Privacy.Namespace, table, table))
		query.WriteString(" ON ")
		query.WriteString(fmt.Sprintf("%s_anon.%s = %s.%s", table, node.PrimaryKey, table, node.PrimaryKey))
	}

	return query.String(), hasQi
}

func (q *QueryBuilder) SelectWithRelationships(namespace string, table string, columns ...string) string {
	query := strings.Builder{}

	node := q.Schema.Nodes[table]
	columns = append(columns, q.ListChildColumns(namespace, table, node)...)
	lhs, hasQi := q.Select(table, columns...)
	query.WriteString(lhs)
	query.WriteString(" ")
	query.WriteString(q.JoinChildren(hasQi, namespace, table, node))

	return query.String()
}

func (q *QueryBuilder) SelectRowIncludeReferences(table string, id int64, columns ...string) string {
	node := q.Schema.Nodes[table]
	query := strings.Builder{}

	query.WriteString(q.SelectWithRelationships(node.Namespace, table, columns...))
	query.WriteString(fmt.Sprintf(" WHERE %s.%s.%s = %v ",
		node.Namespace,
		table,
		node.PrimaryKey,
		id))

	return query.String()
}

func (q *QueryBuilder) ListChildColumns(namespace string, table string, node Node) []string {
	columns := []string{}

	for childTable, childNode := range node.Children {
		columns = append(columns, childNode.Columns...)
		columns = append(columns, q.ListChildColumns(childNode.Namespace, childTable, childNode)...)
	}

	return columns
}

func (q *QueryBuilder) JoinChildren(hasQi bool, namespace string, table string, node Node) string {
	query := strings.Builder{}
	idx := 0
	for name, node := range node.Children {
		if node.Relationship.Type == "" || node.Relationship.ForeignKey.Parent == "" {
			continue
		}

		if idx > 0 {
			query.WriteString(" ")
		}

		var againstTable string
		if hasQi {
			againstTable = fmt.Sprintf("%s_anon", table)
		} else {
			againstTable = fmt.Sprintf("%s.%s", namespace, table)
		}

		query.WriteString(fmt.Sprintf("LEFT JOIN %s ON %s.%s = %s.%s.%s",
			name,
			againstTable,
			node.Relationship.ForeignKey.Parent,
			node.Namespace,
			name,
			node.Relationship.ForeignKey.Child))

		query.WriteString(q.JoinChildren(hasQi, namespace, name, node))
		idx++
	}

	return query.String()
}

func (q *QueryBuilder) decode(data []byte, dataType uint32, format int16) (interface{}, error) {
	if len(data) <= 0 || data == nil {
		return nil, nil
	}

	if dt, ok := q.typeMap.TypeForOID(dataType); ok {
		decoded, err := dt.Codec.DecodeValue(q.typeMap, dataType, format, data)
		if err != nil {
			return nil, err
		}

		switch dataType {
		case pgtype.TimestampOID:
			fallthrough
		case pgtype.TimestamptzOID:
			decoded = decoded.(time.Time).Unix()
		}

		return decoded, nil
	}
	return string(data), nil
}
