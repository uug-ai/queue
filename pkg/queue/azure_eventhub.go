package queue

// AzureEventHub implements QueueInterface through Azure Event Hubs' Kafka
// endpoint. The embedded Kafka client provides publishing, consuming, routing,
// retries, dead-lettering, raw-message handling, and connection management.
type AzureEventHub struct {
	*Kafka
	options *AzureEventHubOptions
}

var _ QueueInterface = (*AzureEventHub)(nil)

func NewAzureEventHub(options *AzureEventHubOptions) (*AzureEventHub, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}
	kafkaOptions, err := options.kafkaOptions()
	if err != nil {
		return nil, err
	}
	kafkaClient, err := NewKafka(kafkaOptions)
	if err != nil {
		return nil, err
	}
	return &AzureEventHub{Kafka: kafkaClient, options: options}, nil
}
