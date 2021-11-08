package verifier

import (
	"log"

	"github.com/Shopify/sarama"
	"github.com/hi-rustin/ticdc-old-value-kafka-integration-test/internal"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/codec"
)

// Consumer represents a Sarama consumer group consumer.
type Consumer struct {
	Ready chan bool
}

// New create a new group consumer.
func New() *Consumer {
	return &Consumer{Ready: make(chan bool)}
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.Ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	count := 0
	log.Printf("Starting process messages...")
	for message := range claim.Messages() {
		rows, err := consumer.decodeDMLEvents(message)
		if err != nil {
			log.Panicf("Decoding DML events failed: %v", err)
		}
		log.Printf("Got %d rows", len(rows))
		count += len(rows)
		for _, row := range rows {
			if !consumer.verifyDMLEvent(row) {
				log.Panic("Verify growing value failed")
			}
		}

		log.Printf("Current count of row messages: %d", count)
		if count == internal.EventNum {
			session.Context().Done()
		}

		session.MarkMessage(message, "")
	}

	return nil
}

// decodeDMLEvents converts the message to a row change events.
func (consumer *Consumer) decodeDMLEvents(message *sarama.ConsumerMessage) ([]*model.RowChangedEvent, error) {
	var rows []*model.RowChangedEvent

	batchDecoder, err := codec.NewJSONEventBatchDecoder(message.Key, message.Value)
	if err != nil {
		return rows, err
	}

	for {
		tp, hasNext, err := batchDecoder.HasNext()
		if err != nil {
			return rows, err
		}
		if !hasNext {
			break
		}
		switch tp {
		case model.MqMessageTypeDDL:
			_, err = batchDecoder.NextDDLEvent()
			if err != nil {
				return rows, err
			}
		case model.MqMessageTypeResolved:
			_, err = batchDecoder.NextResolvedEvent()
			if err != nil {
				return rows, err
			}
		case model.MqMessageTypeRow:
			row, err := batchDecoder.NextRowChangedEvent()
			if err != nil {
				return rows, err
			}
			rows = append(rows, row)
		}
	}

	return rows, nil
}

// verifyDMLEvent checks if the balance change of the received message is 1,
// and also checks if the parity change is normal.
func (consumer *Consumer) verifyDMLEvent(row *model.RowChangedEvent) bool {
	if len(row.Columns) == len(row.PreColumns) {
		for i := 0; i < len(row.Columns); i++ {
			column := row.Columns[i]
			preColumn := row.PreColumns[i]

			if column.Name == internal.BalanceColumnName {
				colValue := column.Value.(int64)
				preColValue := preColumn.Value.(int64)
				log.Printf("preColValue: %d, colValue: %d", preColValue, colValue)

				return preColValue+1 == colValue
			}

			if column.Name == internal.ParityColumnName {
				colValue := string(column.Value.([]byte))
				preColValue := string(preColumn.Value.([]byte))
				log.Printf("preColValue: %s, colValue: %s", preColValue, colValue)
				switch colValue {
				case internal.Odd:
					if preColValue != internal.Even {
						return false
					}
				case internal.Even:
					if preColValue != internal.Odd {
						return false
					}
				default:
					return false
				}
			}
		}
	}

	return true
}
