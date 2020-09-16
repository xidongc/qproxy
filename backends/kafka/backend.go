package kafka

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/wish/qproxy/backends/sqs"
	"github.com/wish/qproxy/config"
	metrics "github.com/wish/qproxy/metrics"
	"github.com/wish/qproxy/rpc"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"os"
	"strconv"
	"time"
)

type Backend struct {

	consumer *kafka.Consumer
	producer *kafka.Producer
	admin 	 *kafka.AdminClient

	DefaultNumParts      int  // Default Number of Partitions in a topic
	DefaultNumReplicas   int  // Default Number of Replicas for each topic
	m metrics.QProxyMetrics
}

func New(conf *config.Config, mets metrics.QProxyMetrics) (*Backend, error) {
	_ = kafka.ConfigMap{
		"enable.idempotence": true,
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	}
	backend := Backend{

	}
	return &backend, nil
}

// create kafka topic
func (s *Backend) CreateQueue(ctx context.Context, in *rpc.CreateQueueRequest) (*rpc.CreateQueueResponse, error) {
	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		panic("ParseDuration(60s)")
	}
	queueName := sqs.QueueIdToName(in.Id)
	numParts := s.DefaultNumParts
	replicationFactor := s.DefaultNumReplicas
	if partitions, ok := in.Attributes["Partitions"]; ok {
		numParts, err = strconv.Atoi(partitions)
	}
	if replications, ok := in.Attributes["Replicas"]; ok {
		replicationFactor, err = strconv.Atoi(replications)
	}
	if numParts < replicationFactor {
		log.Warning("replicas larger than partitions, set equal")
		numParts = replicationFactor
	}

	results, err := s.admin.CreateTopics(
		ctx,
		// Multiple topics can be created simultaneously
		// by providing more TopicSpecification structs here.
		[]kafka.TopicSpecification{{
			Topic:             *queueName,
			NumPartitions:      numParts,
			ReplicationFactor:  replicationFactor}},
		// Admin options
		kafka.SetAdminOperationTimeout(maxDur))
	if err != nil {
		fmt.Printf("Failed to create topic: %v\n", err)
		os.Exit(1)
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			return nil, result.Error
		}
	}
	return &rpc.CreateQueueResponse{}, nil
}

func (s *Backend) PublishMessages(ctx context.Context, in *rpc.PublishMessagesRequest) (*rpc.PublishMessagesResponse, error){
	topic := in.QueueId.String() // use queue id as topic
	deliveryChan := make(chan kafka.Event, len(in.Messages))
	for _, message := range in.Messages {
		s.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte(message.Data),
		}, deliveryChan)
	}

	go func() {
		e := <-deliveryChan
		m := e.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		} else {
			fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
				*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
		}
	}()
}

func (s *Backend) AckMessages(ctx context.Context, in *rpc.AckMessagesRequest) (*rpc.AckMessagesResponse, error){
	topic := in.QueueId.String()
	commitOffset := make([]kafka.TopicPartition, len(in.Receipts))
	// use queue id as topic
	for i, receipt := range in.Receipts {
		partition, err := strconv.ParseInt(receipt.Partition, 10, 32)
		offset, err := strconv.ParseInt(receipt.Id, 10, 64)
		if err != nil {
			log.Error("parse int32 error", err)
		}
		commitOffset[i] = kafka.TopicPartition{
			Topic:     &topic,
			Partition: int32(partition),
			Offset:    kafka.Offset(offset),
			Metadata:  nil,
		}
	}

	res, err := s.consumer.CommitOffsets(commitOffset)
	if err != nil {
		log.Error("commit offset error", err)
	}

}

func (s *Backend) GetMessages(ctx context.Context, in *rpc.GetMessagesRequest) (*rpc.GetMessagesResponse, error) {
	err := s.consumer.SubscribeTopics([]string{in.QueueId.String()}, nil)
	if err != nil {
		log.Error("subscribe topic ", err)
	}

	msg, err := s.consumer.ReadMessage(-1)
	if err == nil {
		fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
	} else {
		// The client will automatically try to recover from all errors.
		fmt.Printf("Consumer error: %v (%v)\n", err, msg)
	}

}

func (s *Backend) Healthcheck(ctx context.Context, in *rpc.HealthcheckRequest) (*rpc.HealthcheckResponse, error) {
	return &rpc.HealthcheckResponse{}, nil
}