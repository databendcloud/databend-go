# databend-go
Golang driver for [databend cloud](https://www.databend.com/)

## Key features

- Supports native Databend HTTP client-server protocol
- Compatibility with [`database/sql`](#std-databasesql-interface)

# Examples

## Connecting
Connection can be achieved either via a DSN string with the format `https://user:password@host/database?<query_option>=<value>` and sql/Open method such as `https://username:password@tenant--warehousename.ch.datafusecloud.com/test`.

```go
import (
  "database/sql"
  _ "github.com/databendcloud/databend-go"
)

func ConnectDSN() error {
    dsn, cfg, err := getDSN()
    if err != nil {
    log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
    }
    conn, err := sql.Open("databend", dsn)
    if err != nil {
        return err
    }
    return conn.Ping()
}
```

## Connection Settings
- host - the connect host such as `tenant--warehousename.ch.datafusecloud.com` that you can get from databend cloud as follows:
	<img width="1084" alt="image" src="https://user-images.githubusercontent.com/7600925/201461064-d503cfb3-43e0-4c7c-b270-2898452ebc8e.png">

- username/password - auth credentials that you can get from databend cloud connect page as above
- database - select the current default database


## Execution
Once a connection has been obtained, users can issue sql statements for execution via the Exec method.

```go
    dsn, cfg, err := getDSN()
	if err != nil {
		log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
	}
	conn, err := sql.Open("databend", dsn)
	if err != nil {
		fmt.Println(err)
	}
	conn.Exec(`DROP TABLE IF EXISTS data`)
	_, err = conn.Exec(`
    CREATE TABLE IF NOT EXISTS  data(
        Col1 TINYINT,
        Col2 VARCHAR 
    ) 
    `)
	if err != nil {
		fmt.Println(err)
	}
	_, err = conn.Exec("INSERT INTO data VALUES (1, 'test-1')")
```

## Querying Row/s
Querying a single row can be achieved using the QueryRow method. This returns a *sql.Row, on which Scan can be invoked with pointers to variables into which the columns should be marshaled. 

```go
dsn, cfg, err := getDSN()
	if err != nil {
		log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
	}
	conn, err := sql.Open("databend", dsn)
	if err != nil {
		fmt.Println(err)
	}
	row := conn.QueryRow("SELECT * FROM data")
	var (
		col1 uint8
		col2 string
	)
	if err := row.Scan(&col1, &col2); err != nil {
		fmt.Println(err)
	}
	fmt.Println(col2)
```

Iterating multiple rows requires the Query method. This returns a *sql.Rows struct on which Next can be invoked to iterate through the rows. QueryContext equivalent allows passing of a context.

```go
dsn, cfg, err := getDSN()
	if err != nil {
		log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
	}
	conn, err := sql.Open("databend", dsn)
	if err != nil {
		fmt.Println(err)
	}
	row, err := conn.Query("SELECT * FROM data")
	var (
		col1 uint8
		col2 string
	)
	for row.Next() {
		if err := row.Scan(&col1, &col2); err != nil {
			fmt.Println(err)
		}
		fmt.Println(col2)
	}
```
