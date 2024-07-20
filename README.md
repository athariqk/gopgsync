# gopgsync

Gopgsync is a PostgreSQL CDC for syncing data to any search engine sinks (currently meilisearch only) written in Go.

## Usage

Gopgsync requires a `schema.yaml` file to define how gopgsync should replicate the database.

The typical `schema.yaml` file looks like the following:
```yaml
nodes:
  table_1:
    index: <search engine's index>
    pk: <table's primary key>
    sync: "none" | "init" | "all"
    columns:
      - <column 1>
      - <column 2>
      ...
  table_2:
    columns:
      - <column 1>
      - <column 2>
      ...
    children:
      child_table_1:
        columns:
          - <column 1>
          - <column 2>
          ...
        transform:
          rename:
            old_column: "<new column>"
        relationship:
          type: "one_to_one" | "one_to_many"
          fk:
            child: <this node's primary key>
            parent: <parent's foreign key>
      child_table_2:
        pk: <table's primary key>
        columns:
          - <column 1>
          - <column 2>
          ...
```

### `nodes`

An object node describing the search engine document. Root-level tables lives here.

### `index`

An optional search engine index (defaults to database name)

### `children`

An optional list of child nodes if any. This has the same structure as a parent node. Also defines relationship.

### `sync`

Specifies how the table should be synchronized.
- `none`: don't sync this table altogether
- `init`: only sync this table on start-up (for populating the search engine db)
- `all`: sync this table using PostgreSQL's logical replication

### `columns`

An optional list of table columns to replicate (defaults to all).

### `relationship`

Describes the relationship between parent and child.
- `type`: type can be `one_to_one` or `one_to_many` depending on the relationship type between parent and child
- `fk`: specifies the foreign keys of the relationship

### `transform`

List of table-level transform operators.
- `rename`: renames `old_column` name to the specified `new_column` name

## Command-line Arguments

### stream

Run gopgysnc in logical streaming replication protocol, this is the default replication mode.

### full

Fully replicates the schema to the target sink. Useful when you're trying to replicate for the first time and need to "populate" the target database.