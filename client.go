package gobulk

import (
	"database/sql"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

type Client struct {
	DbmsName  string
	InputUrl  string
	OutputUrl string
}

type Column struct {
	Field   string
	Type    string
	Null    string
	Key     string
	Default *string
	Extra   string
}

var tableName string
var columnName string

func NewClient(DbmsName string, InputUrl string, OutputUrl string) *Client {
	return &Client{DbmsName: DbmsName, InputUrl: InputUrl, OutputUrl: OutputUrl}
}

func (c *Client) Sync() error {
	// input
	inputDb, err := sql.Open(c.DbmsName, c.InputUrl)
	if err != nil {
		return err
	}
	err = inputDb.Ping()
	if err != nil {
		return err
	}

	// output
	outputDb, err := sql.Open(c.DbmsName, c.OutputUrl)
	if err != nil {
		return err
	}
	err = outputDb.Ping()
	if err != nil {
		return err
	}
	defer inputDb.Close()
	defer outputDb.Close()

	tables, err := inputDb.Query("show tables")
	if err != nil {
		return err
	}
	for tables.Next() {
		err := tables.Scan(&tableName)
		if err != nil {
			return err
		}
		log.Println("=== table:" + tableName + " ===")
		columns, err := inputDb.Query("show columns from " + tableName)
		for columns.Next() {
			var column Column
			err := columns.Scan(&column.Field, &column.Type, &column.Null, &column.Key, &column.Default, &column.Extra)
			if err != nil {
				return err
			}
			log.Println("column: " + column.Field)
		}
	}
	return nil
}
