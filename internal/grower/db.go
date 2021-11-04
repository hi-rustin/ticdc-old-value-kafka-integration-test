package grower

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

// We only support the mysql driver.
const driverName = "mysql"

const (
	initBalance = 1
	firstRowID  = 1
)

const (
	createTable = "CREATE TABLE test.balances(id INT PRIMARY KEY AUTO_INCREMENT, balance INT)"
	insert      = "INSERT INTO balances(balance) VALUES (?);"
	increment   = "UPDATE balances SET balance = balance + 1 WHERE id = ?"
)

// Conn holds the database connection.
type Conn struct {
	db             *sql.DB
	currentBalance int
}

// New a connection to the database.
func New(dataSourceName string) (*Conn, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	return &Conn{db: db, currentBalance: initBalance}, nil
}

func (c *Conn) CreateTable() error {
	_, err := c.db.Exec(createTable)
	if err != nil {
		return err
	}

	return nil
}

func (c *Conn) Init() error {
	_, err := c.db.Exec(insert, c.currentBalance)
	if err != nil {
		return err
	}

	return nil
}

func (c *Conn) Increment() error {
	_, err := c.db.Exec(increment, firstRowID)
	if err != nil {
		return err
	}
	c.currentBalance++

	return nil
}

func (c *Conn) Close() error {
	return c.db.Close()
}
