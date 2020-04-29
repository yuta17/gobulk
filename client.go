package gobulk

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// Client .
type Client struct {
	InputDB  *sql.DB
	OutputDB *sql.DB
}

// Column .
type Column struct {
	Field   string
	Type    string
	Null    string
	Key     string
	Default *string
	Extra   string
}

// NewClient .
func NewClient(DbmsName, InputURL, OutputURL string) (*Client, error) {
	inputDb, err := inputDB(DbmsName, InputURL)
	if err != nil {
		return nil, err
	}
	outputDb, err := outputDB(DbmsName, OutputURL)
	if err != nil {
		return nil, err
	}
	return &Client{InputDB: inputDb, OutputDB: outputDb}, nil
}

func inputDB(dbmsName, inputURL string) (*sql.DB, error) {
	inputDb, err := sql.Open(dbmsName, inputURL)
	if err != nil {
		return nil, err
	}
	err = inputDb.Ping()
	if err != nil {
		return nil, err
	}
	return inputDb, nil
}

func outputDB(dbmsName, outputURL string) (*sql.DB, error) {
	outputDb, err := sql.Open(dbmsName, outputURL)
	if err != nil {
		return nil, err
	}
	err = outputDb.Ping()
	if err != nil {
		return nil, err
	}
	return outputDb, nil
}

// Sync .
func (c *Client) Sync() error {
	defer c.InputDB.Close()
	defer c.OutputDB.Close()

	tables, err := c.InputDB.Query("show tables")
	if err != nil {
		return err
	}

	var tableName string
	for tables.Next() {
		err := tables.Scan(&tableName)
		if err != nil {
			return err
		}

		log.Println("=== table: " + tableName + " ===")

		err = c.createTableIfNotExisted(tableName)
		if err != nil {
			return err
		}

		rows, err := c.InputDB.Query("select * from " + tableName)
		if err != nil {
			return err
		}
		if !rows.Next() {
			continue
		}
		columnNames, err := rows.Columns()
		if err != nil {
			return err
		}
		values, err := c.getValues(rows, columnNames)
		if err != nil {
			return err
		}

		if len(values) != 0 {
			err := c.execUpsertQuery(tableName, columnNames, values)
			if err != nil {
				return err
			}
		}
		rows.Close()
	}
	tables.Close()

	return nil
}

func (c *Client) createTableIfNotExisted(tableName string) error {
	_, notExistedErr := c.OutputDB.Query("select * from " + tableName + " limit 1")
	columns, err := c.InputDB.Query("show columns from " + tableName)
	if err != nil {
		return err
	}

	createColumns := []string{}
	for columns.Next() {
		var column Column
		err := columns.Scan(&column.Field, &column.Type, &column.Null, &column.Key, &column.Default, &column.Extra)
		if err != nil {
			return err
		}

		if notExistedErr != nil {
			var notNullStr string
			var priStr string
			if column.Key == "YES" {
				notNullStr = "not null"
			} else {
				notNullStr = ""
			}

			if column.Key == "PRI" {
				priStr = "primary key"
			} else {
				priStr = ""
			}

			createColumns = append(createColumns, fmt.Sprintf("`%s` %s %s %s", column.Field, column.Type, notNullStr, priStr))
		}
	}

	if notExistedErr != nil {
		createTableQuery := fmt.Sprintf("create table %s (%s)", tableName, strings.Join(createColumns, ","))
		_, err := c.OutputDB.Exec(createTableQuery)
		if err != nil {
			return err
		}
		log.Println(fmt.Sprintf("%s table has been created.", tableName))
	}
	columns.Close()
	return nil
}

func (c *Client) getValues(rows *sql.Rows, columnNames []string) ([]string, error) {
	count := len(columnNames)
	values := make([]interface{}, count)
	valuePointers := make([]interface{}, count)

	rowvals := []string{}

	for rows.Next() {
		for i := range columnNames {
			valuePointers[i] = &values[i]
		}

		err := rows.Scan(valuePointers...)
		if err != nil {
			return nil, err
		}

		r := []string{}
		for i := range columnNames {
			val := values[i]
			b, ok := val.([]byte)
			var v string
			if ok {
				v = string(b)
				v = "'" + v + "'"
			}
			if b == nil {
				v = "NULL"
			}
			if i == 0 {
				v = "(" + v
			}
			if i == (len(columnNames) - 1) {
				v = v + ")"
			}
			r = append(r, v)
		}

		insertValues := strings.Join(r, ",")
		rowvals = append(rowvals, insertValues)
	}
	return rowvals, nil
}

func (c *Client) execUpsertQuery(tableName string, columnNames, values []string) error {
	updateValues := []string{}
	columnNamesWithBackQuote := []string{}

	for _, columnName := range columnNames {
		columnNamesWithBackQuote = append(columnNamesWithBackQuote, "`"+columnName+"`")
		v := fmt.Sprintf("`%s` = values(`%s`)",
			columnName,
			columnName)
		updateValues = append(updateValues, v)
	}

	upsertQuery := fmt.Sprintf(`insert into %s (%s) values %s on duplicate key update %s`,
		tableName,
		strings.Join(columnNamesWithBackQuote, ","),
		strings.Join(values, ","),
		strings.Join(updateValues, ","))

	_, err := c.OutputDB.Exec(upsertQuery)
	if err != nil {
		return err
	}

	recordCount := len(values)
	log.Println(fmt.Sprintf("Done. Upserted records count: %s", strconv.Itoa(recordCount)))
	return nil
}
