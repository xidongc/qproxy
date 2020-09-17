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
	"strings"
	"time"
)

type Backend struct {

	consumer *kafka.Consumer
	producer *kafka.Producer
	admin 	 *kafka.AdminClient

	DefaultNumParts      int  // Default Number of Partitions in a topic
	DefaultNumReplicas   int  // Default Number of Replicas for each topic
	m metrics.QProxyMetrics

	enableIdempotence    bool
	adminTimeoutSeconds  int
}

func New(conf *config.Config, mets metrics.QProxyMetrics) (*Backend, error) {
	_ = kafka.ConfigMap{
		"enable.idempotence": 	 true,
		"bootstrap.servers": 	 conf.Region,  // required, use region for now, TODO
		"group.id":          	 "myGroup",  // required
		"auto.offset.reset": 	 "earliest",
		"queuing.strategy":  	 "fifo",
		"message.timeout.ms":    300000,
		"request.required.acks": -1,
	}
	backend := Backend{

	}
	return &backend, nil
}

// create kafka topic
func (s *Backend) CreateQueue(ctx context.Context, in *rpc.CreateQueueRequest) (resp *rpc.CreateQueueResponse, err error) {
	maxDur := time.Duration(s.adminTimeoutSeconds) * time.Second
	queueName := sqs.QueueIdToName(in.Id)
	numParts := s.DefaultNumParts
	replicationFactor := s.DefaultNumReplicas
	if partitions, ok := in.Attributes["Partitions"]; ok {
		numParts, err = strconv.Atoi(partitions)
		if err != nil {
			return nil, err
		}
	}
	if replications, ok := in.Attributes["Replicas"]; ok {
		replicationFactor, err = strconv.Atoi(replications)
		if err != nil {
			return nil, err
		}
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

// delete kafka topic
func (s *Backend) DeleteQueue(ctx context.Context, in *rpc.DeleteQueueRequest) (*rpc.DeleteQueueResponse, error) {
	topics := make([]string, 1)
	topics[0] = *sqs.QueueIdToName(in.Id)
	maxDur := time.Duration(s.adminTimeoutSeconds) * time.Second
	results, err := s.admin.DeleteTopics(ctx, topics, kafka.SetAdminOperationTimeout(maxDur))
	if err != nil {
		log.Error(err)
		return nil, err
	}
	// Print results
	for _, result := range results {
		fmt.Printf("%s\n", result)
	}
	return &rpc.DeleteQueueResponse{}, nil
}

// get topic attributes
func (s *Backend) GetQueue(ctx context.Context, in *rpc.GetQueueRequest) (*rpc.GetQueueResponse, error) {
	resourceType, err := kafka.ResourceTypeFromString("topic")
	queueName := sqs.QueueIdToName(in.Id)
	results, err := s.admin.DescribeConfigs(ctx, []kafka.ConfigResource{
		{
			Type: resourceType,
			Name: *queueName,
		},
	})
	if err != nil {
		fmt.Printf("Failed to DescribeConfigs(%s, %s): %s\n",
			resourceType, queueName, err)
		return nil, err
	}
	resp := &rpc.GetQueueResponse{}
	queueAttributes := make(map[string]string, len(results))
	for _, result := range results {
		fmt.Printf("%s %s: %s:\n", result.Type, result.Name, result.Error)
		for _, entry := range result.Config {
			// Truncate the value to 60 chars, if needed, for nicer formatting.
			fmt.Printf("%60s = %-60.60s   %-20s Read-only:%v Sensitive:%v\n",
				entry.Name, entry.Value, entry.Source,
				entry.IsReadOnly, entry.IsSensitive)
			queueAttributes[entry.Name] = entry.Value
		}
	}
	resp.Attributes = queueAttributes
	return resp, nil
}

// public messages
func (s *Backend) PublishMessages(ctx context.Context, in *rpc.PublishMessagesRequest) (*rpc.PublishMessagesResponse, error){
	resp := &rpc.PublishMessagesResponse{}
	topic := in.QueueId.String() // use queue id as topic
	deliveryChan := make(chan kafka.Event, len(in.Messages))
	failed := make([]*rpc.FailedPublish, 0)
	for _, message := range in.Messages {
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte(message.Data),
		}
		if err := s.producer.Produce(msg, deliveryChan); err != nil {
			failed = append(failed, &rpc.FailedPublish{
				Index:                int64(msg.TopicPartition.Offset),
				ErrorMessage:         err.Error(),
			})
		}
	}

	go func() {
		e := <-deliveryChan
		m := e.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
			failed = append(failed, &rpc.FailedPublish{
				Index:                int64(m.TopicPartition.Offset),
				ErrorMessage:         m.TopicPartition.Error.Error(),
			})
		} else {
			fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
				*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
		}
	}()
	resp.Failed = failed
	return resp, nil
}

// ack message to broker
func (s *Backend) AckMessages(ctx context.Context, in *rpc.AckMessagesRequest) (*rpc.AckMessagesResponse, error){
	resp := &rpc.AckMessagesResponse{}
	failed := make([]*rpc.MessageReceipt, 0)
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

	results, err := s.consumer.CommitOffsets(commitOffset)
	if err != nil {
		log.Error("commit offset error", err)
		return nil, err
	}
	for _, result := range results {
		if result.Error != nil {
			failed = append(failed, &rpc.MessageReceipt{
				Id:           *result.Topic,
				Partition:    strconv.Itoa(int(result.Partition)),
				ErrorMessage: result.Error.Error(),
			})
		}
	}
	resp.Failed = failed
	return resp, nil
}

// get messages in long polling
func (s *Backend) GetMessages(ctx context.Context, in *rpc.GetMessagesRequest) (*rpc.GetMessagesResponse, error) {
	resp := &rpc.GetMessagesResponse{}
	err := s.consumer.SubscribeTopics([]string{in.QueueId.String()}, nil)
	if err != nil {
		log.Error("subscribe topic ", err)
		return nil, err
	}
	elapse := 0 * time.Second
	messages := make([]*rpc.Message, 0)
	for elapse <= time.Duration(in.LongPollSeconds) * time.Second {
		start := time.Now()
		msg, err := s.consumer.ReadMessage(time.Duration(in.AckDeadlineSeconds) * time.Second)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			return nil, err
		}
		attributes := make(map[string]string)
		attributes["Timestamp"] = msg.Timestamp.String()
		attributes["TimestampType"] = msg.TimestampType.String()
		attributes["Keys"] = string(msg.Key)
		attributes["Headers"] = strings.Join(Map(msg.Headers), "-")

		partition := strconv.Itoa(int(msg.TopicPartition.Partition))

		messages = append(messages, &rpc.Message{
			Data:                 string(msg.Value),
			Attributes:           attributes,
			Receipt:              &rpc.MessageReceipt{
				Id: *msg.TopicPartition.Topic,
				Partition:  partition,
			},
		})
		elapse += time.Since(start)
	}
	resp.Messages = messages
	return resp, nil
}

func (s *Backend) ListQueues(*rpc.ListQueuesRequest, rpc.QProxy_ListQueuesServer) error {
	panic("implement me")
}

func (s *Backend) ModifyQueue(ctx context.Context, in *rpc.ModifyQueueRequest) (*rpc.ModifyQueueResponse, error) {
	panic("implement me")
}

// purge queue following steps in https://stackoverflow.com/questions/16284399/purge-kafka-topic
func (s *Backend) PurgeQueue(ctx context.Context, in *rpc.PurgeQueueRequest) (*rpc.PurgeQueueResponse, error) {
	err := s.consumer.Pause([]kafka.TopicPartition{
		{
			Topic: sqs.QueueIdToName(in.Id),
			Partition: kafka.PartitionAny,
		},
	})
	if err != nil {
		log.Errorf("Pause failed: %s", err)
		return nil, err
	}
	_, err = s.DeleteQueue(ctx, &rpc.DeleteQueueRequest{
		Id: in.Id,
		RPCTimeout: in.RPCTimeout,
	})
	if err != nil {
		log.Errorf("topic delete failed: %s", err)
		return nil, err
	}

	// TODO add rpcTimeout
	_, err = s.CreateQueue(ctx, &rpc.CreateQueueRequest{
		Id:                   in.Id,
		RPCTimeout:           in.RPCTimeout,
	})
	if err != nil {
		log.Errorf("topic re-create failed: %s", err)
		return nil, err
	}
	return &rpc.PurgeQueueResponse{}, nil
}

// As in a kafka consumer group, a partition can only be consumed by one consumer
// msg will not be routed to other consumer unless re-balance, also, group
// coordinator will maintain heartbeat with consumer in case its failed
func (s *Backend) ModifyAckDeadline(ctx context.Context, in *rpc.ModifyAckDeadlineRequest) (res *rpc.ModifyAckDeadlineResponse, err error) {
	return
}

// health check
func (s *Backend) Healthcheck(ctx context.Context, in *rpc.HealthcheckRequest) (*rpc.HealthcheckResponse, error) {
	return &rpc.HealthcheckResponse{}, nil
}
