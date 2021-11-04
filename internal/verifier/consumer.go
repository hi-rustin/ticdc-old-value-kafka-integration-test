package verifier

import (
	"log"

	"github.com/Shopify/sarama"
	"github.com/pingcap/ticdc/cdc/model"
)

// Consumer represents a Sarama consumer group consumer.
type Consumer struct {
	ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		session.MarkMessage(message, "")
	}

	return nil
}

func (consumer *Consumer) decodeDMLEvent(message *sarama.ConsumerMessage) error {
	panic("unimplemented")
}

func (consumer *Consumer) verifyDMLEvent(row *model.RowChangedEvent) bool {
	panic("unimplemented")
}
