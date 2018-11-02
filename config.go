package qproxy

import (
	"github.com/jessevdk/go-flags"
	"log"
	"time"
)

const (
	SQS    string = "sqs"
	Pubsub string = "pubsub"
)

type Config struct {
	Backend string `long:"backend" description:"Backend queueing system to use" required:"true"`
	Region  string `long:"region" description:"Region to connect to (if applicable)"`

	GRPCPort int `long:"grpcport" description:"Port for grpc server to listen on" default:"8887"`
	HTTPPort int `long:"httpport" description:"Port for http server to listen on" default:"8888"`

	WriteTimeout time.Duration `long:"writetimeout" description:"HTTP server write timeout" default:"0"`
	ReadTimeout  time.Duration `long:"readtimeout" description:"HTTP server read timeout" default:"0"`
	IdleTimeout  time.Duration `long:"idletimeout" description:"HTTP server idle timeout" default:"0"`

	TermSleep time.Duration `long:"termsleep" description:"How long to sleep before gracefully shutting down" default:"30s"`

	Profile    string `long:"profile" description:"Run a CPUProfile, output a file with this name"`
	MemProfile string `long:"memprofile" description:"Run a MemProfile, output a file with this name"`
}

func ParseConfig() *Config {
	var config Config
	parser := flags.NewParser(&config, flags.Default)
	if _, err := parser.Parse(); err != nil {
		log.Fatalf(err.Error())
	}
	return &config
}