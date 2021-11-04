module github.com/hi-rustin/ticdc-old-value-kafka-integration-test

go 1.16

require (
	github.com/Shopify/sarama v1.27.2
	github.com/go-sql-driver/mysql v1.6.0
	github.com/pingcap/ticdc v0.0.0-20211104084901-390db3871242
)

replace google.golang.org/grpc v1.40.0 => google.golang.org/grpc v1.29.1
