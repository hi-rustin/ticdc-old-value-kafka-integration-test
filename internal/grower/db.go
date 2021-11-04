package grower

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

// We only support the mysql driver.
const driverName = "mysql"

// Conn holds the database connection.
type Conn struct {
	db *sql.DB
}

// New a connection to the database.
func New(dataSourceName string) (*Conn, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	return &Conn{db: db}, nil
}

func (c *Conn) createTable() error {
	panic("unimplemented")
}

func (c *Conn) increment() error {
	panic("unimplemented")
}

func (c *Conn) close() error {
	return c.db.Close()
}
