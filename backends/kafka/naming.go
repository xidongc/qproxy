package kafka

import "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"

func Map(vs []kafka.Header) []string {
	vsm := make([]string, len(vs))
	for i, v := range vs {
		vsm[i] = v.String()
	}
	return vsm
}
