package main

import (
	"flag"
	"log"
	"os"

	"github.com/go-sql-driver/mysql"
	"github.com/hi-rustin/ticdc-old-value-kafka-integration-test/internal"
	"github.com/hi-rustin/ticdc-old-value-kafka-integration-test/internal/grower"
)

type options struct {
	DSN string
}

// validate validates grower options.
func (o *options) validate() error {
	_, err := mysql.ParseDSN(o.DSN)
	if err != nil {
		return err
	}

	return nil
}

func gatherOptions() options {
	o := options{}
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.StringVar(&o.DSN, "dsn", "root@tcp(localhost:4000)/test", "Data Source Name.")
	_ = fs.Parse(os.Args[1:])
	return o
}

func main() {
	o := gatherOptions()
	if err := o.validate(); err != nil {
		log.Fatalf("Invalid options: %v", err)
	}

	conn, err := grower.New(o.DSN)
	defer func(conn *grower.Conn) {
		_ = conn.Close()
	}(conn)

	if err != nil {
		log.Fatalf("Connection to database failed: %v", err)
	}

	err = conn.CreateTable()
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	err = conn.Init()
	if err != nil {
		log.Fatalf("Failed to init table: %v", err)
	}

	for i := 0; i < internal.EventNum; i++ {
		err = conn.Increment()
		if err != nil {
			log.Fatalf("Failed to grow data: %v", err)
		}
	}
}
