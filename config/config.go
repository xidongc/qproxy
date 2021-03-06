package config

import (
	"log"
	"time"

	"github.com/jessevdk/go-flags"
)

const (
	SQS    string = "sqs"
	Pubsub string = "pubsub"
	Kafka  string = "kafka"
)

type Config struct {
	Backend           string        `long:"backend" description:"Backend queueing system to use" required:"true"`
	Region            string        `long:"region" description:"Region to connect to (if applicable)"`
	MetricsMode       bool          `long:"metricsmode" description:"Start qproxy in metrics mode, to collect queued/inflight metrics per queue"`
	MetricsNamespace  string        `long:"metricsnamespace" description:"What namespace to collect additional metrics under" default:"prod"`
	MaxIdleConns      int           `long:"maxidleconns" description:"Maximum number of connections to hold to the backend" default:"1000"`
	MaxConnsPerHost   int           `long:"maxconnsperhost" description:"Maximum number of connections to have open to the backend" default:"10000"`
	DefaultRPCTimeout time.Duration `long:"defaultrpctimeout" description:"Default rpc timeout if none specified" default:"30s"`

	GRPCPort int `long:"grpcport" description:"Port for grpc server to listen on" default:"8887"`
	HTTPPort int `long:"httpport" description:"Port for http server to listen on" default:"8888"`

	WriteTimeout time.Duration `long:"writetimeout" description:"HTTP server write timeout" default:"0"`
	ReadTimeout  time.Duration `long:"readtimeout" description:"HTTP server read timeout" default:"0"`
	IdleTimeout  time.Duration `long:"idletimeout" description:"HTTP server idle timeout" default:"0"`

	TermSleep time.Duration `long:"termsleep" description:"How long to sleep before gracefully shutting down" default:"30s"`

	Profile    string `long:"profile" description:"Run a CPUProfile, output a file with this name"`
	MemProfile string `long:"memprofile" description:"Run a MemProfile, output a file with this name"`

	// TODO Adding Kafka specific settings, better put it in sub command
	EnableIdempotence    bool    `long:"idempotence" description:"enable idempotence" `
	Servers              string  `long:"servers" description:"kafka servers to connect" default:"localhost"`
	QueueStrategy 	     string  `long:"strategy" description:"kafka queue strategy to use" default:"fifo"`
	AdminTimeoutSeconds  int     `long:"admintimeout" description:"admin timeout in seconds" default:"60"`
	DefaultNumParts	     int     `long:"parts" description:"partitions number for each topic" default:"1"`
	DefaultNumReplicas   int     `long:"replicas" description:"replicas number for each topic" default:"1"`
}

func ParseConfig() *Config {
	var config Config
	parser := flags.NewParser(&config, flags.Default)
	if _, err := parser.Parse(); err != nil {
		log.Fatalf(err.Error())
	}
	return &config
}
