package grower

import (
	"database/sql"

	// MySQL driver for database/sql.
	_ "github.com/go-sql-driver/mysql"
	"github.com/hi-rustin/ticdc-old-value-kafka-integration-test/internal"
)

// We only support the mysql driver.
const driverName = "mysql"

const (
	// Initialize the balance.
	initBalance = 1
	// The ID of the first row, automatically generated, defaults to 1.
	firstRowID = 1
)

const (
	// Balances table.
	createBalancesTable = "CREATE TABLE test.balances(id INT PRIMARY KEY AUTO_INCREMENT, balance INT);"
	insertBalance       = "INSERT INTO balances(balance) VALUES (?);"
	incrementBalance    = "UPDATE balances SET balance = balance + 1 WHERE id = ?;"

	// Balances parity table.
	createBalancesParitiesTable = `CREATE TABLE test.balances_parities
	(id INT PRIMARY KEY AUTO_INCREMENT, parity VARCHAR(255));`
	insertBalanceParity = "INSERT INTO balances_parities(parity) VALUES (?);"
	updateBalanceParity = "UPDATE balances_parities SET parity = ? WHERE id = ?;"
)

// Conn holds the database connection.
type Conn struct {
	db *sql.DB
	// The current balance that has been received.
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

// CreateTable creates balances and balances_parities tables.
func (c *Conn) CreateTable() error {
	_, err := c.db.Exec(createBalancesTable)
	if err != nil {
		return err
	}

	_, err = c.db.Exec(createBalancesParitiesTable)
	if err != nil {
		return err
	}

	return nil
}

// Init inits balances and balances_parities tables.
func (c *Conn) Init() error {
	_, err := c.db.Exec(insertBalance, c.currentBalance)
	if err != nil {
		return err
	}

	_, err = c.db.Exec(insertBalanceParity, internal.Odd)
	if err != nil {
		return err
	}

	return nil
}

// Increment gradually increases the balance, each time for 1.
func (c *Conn) Increment() error {
	txn, err := c.db.Begin()
	if err != nil {
		return err
	}
	_, err = txn.Exec(incrementBalance, firstRowID)
	if err != nil {
		_ = txn.Rollback()
		return err
	}
	c.currentBalance++

	parity := internal.Even
	if c.currentBalance&1 == 0 {
		parity = internal.Odd
	}

	_, err = txn.Exec(updateBalanceParity, parity, firstRowID)
	if err != nil {
		_ = txn.Rollback()
		return err
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	return nil
}

// Close closes database connection.
func (c *Conn) Close() error {
	return c.db.Close()
}
