# gopgsync

Gopgsync is a PostgreSQL CDC for syncing data to any sinks (currently meilisearch only)

## Usage

Gopgsync requires a `schema.yaml` file to define how gopgsync should replicate the database.

The typical `schema.yaml` file looks like the following:
```yaml
sync:
  offers:
    columns:
      name:
    relationships:
      - key: "organization_id"
        table: "organizations"
        columns:
          name:
        transform:
          rename:
            name: "organization"
      - key: "location_id"
        table: "locations"
        primaryKey: "district_id"
        columns:
          address:
```

- `sync`: list of tables to sync
- `columns`: list of table fields to sync
- `relationships`: defines the relationships of the table 
- `transform`: list of transform operators
