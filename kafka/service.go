// Package kafka implements a Functions Kafka middleware for use by
// scaffolding which exposes a function as a service that consumes
// messages from Kafka topics.
package kafka

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
)

const (
	DefaultLogLevel      = LogDebug
	DefaultListenAddress = "[::]:8080"
)

const (
	ServerShutdownTimeout = 30 * time.Second
	InstanceStopTimeout   = 30 * time.Second
)

// Start an instance using a new Service.
func Start(f Handler) error {
	log.Debug().Msg("func runtime creating function instance")
	return New(f).Start(context.Background())
}

// Service exposes a Function Instance as a Kafka consumer with HTTP health
// endpoints.
type Service struct {
	http.Server
	listener net.Listener
	stop     chan error
	f        Handler
	ready    atomic.Bool
}

// New Service which serves the given instance.
func New(f Handler) *Service {
	svc := &Service{
		f:    f,
		stop: make(chan error, 1),
		Server: http.Server{
			ReadTimeout:       30 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       30 * time.Second,
			MaxHeaderBytes:    1 << 20,
			ReadHeaderTimeout: 2 * time.Second,
		},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health/readiness", svc.Ready)
	mux.HandleFunc("/health/liveness", svc.Alive)
	svc.Handler = mux

	logImplements(f)

	return svc
}

func logImplements(f any) {
	if _, ok := f.(Starter); ok {
		log.Info().Msg("Function implements Start")
	}
	if _, ok := f.(Stopper); ok {
		log.Info().Msg("Function implements Stop")
	}
	if _, ok := f.(ReadinessReporter); ok {
		log.Info().Msg("Function implements Ready")
	}
	if _, ok := f.(LivenessReporter); ok {
		log.Info().Msg("Function implements Alive")
	}
}

// Start the service. Blocks until the context is canceled, a runtime error
// occurs, or an OS interrupt/kill signal is received.
func (s *Service) Start(ctx context.Context) (err error) {
	addr := listenAddress()
	log.Debug().Str("address", addr).Msg("function starting")

	if s.listener, err = net.Listen("tcp", addr); err != nil {
		return
	}

	if err = s.startInstance(ctx); err != nil {
		return
	}

	s.handleSignals()

	// Start HTTP health server
	go func() {
		if err := s.Serve(s.listener); err != http.ErrServerClosed {
			log.Error().Err(err).Msg("http server exited with unexpected error")
			s.stop <- err
		}
	}()

	// Start Kafka consumer
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()
	go func() {
		if err := s.consumeLoop(consumerCtx); err != nil {
			log.Error().Err(err).Msg("kafka consumer exited with error")
			s.stop <- err
		}
	}()

	log.Debug().Msg("waiting for stop signals or errors")
	select {
	case err = <-s.stop:
		if err != nil {
			log.Error().Err(err).Msg("function error")
		}
	case <-ctx.Done():
		log.Debug().Msg("function canceled")
	}
	consumerCancel()
	return s.shutdown(err)
}

// Addr returns the address upon which the service is listening if started;
// nil otherwise.
func (s *Service) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Ready handles readiness checks.
func (s *Service) Ready(w http.ResponseWriter, r *http.Request) {
	if i, ok := s.f.(ReadinessReporter); ok {
		ready, err := i.Ready(r.Context())
		if err != nil {
			log.Debug().Err(err).Msg("error checking readiness")
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, "error checking readiness: ", err.Error())
			return
		}
		if !ready {
			log.Debug().Msg("function not yet ready")
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintln(w, "function not yet ready")
			return
		}
	} else if !s.ready.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintln(w, "kafka consumer not yet ready")
		return
	}
	fmt.Fprintf(w, "READY")
}

// Alive handles liveness checks.
func (s *Service) Alive(w http.ResponseWriter, r *http.Request) {
	if i, ok := s.f.(LivenessReporter); ok {
		alive, err := i.Alive(r.Context())
		if err != nil {
			log.Err(err).Msg("error checking liveness")
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, "error checking liveness: ", err.Error())
			return
		}
		if !alive {
			log.Debug().Msg("function not alive")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("function not alive"))
			return
		}
	}
	fmt.Fprintf(w, "ALIVE")
}

func (s *Service) startInstance(ctx context.Context) error {
	if i, ok := s.f.(Starter); ok {
		cfg, err := newCfg()
		if err != nil {
			return err
		}
		go func() {
			if err := i.Start(ctx, cfg); err != nil {
				s.stop <- err
			}
		}()
	} else {
		log.Debug().Msg("function does not implement Start. Skipping")
	}
	return nil
}

func (s *Service) handleSignals() {
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs)
	go func() {
		for {
			sig := <-sigs
			if sig == syscall.SIGINT || sig == syscall.SIGTERM {
				log.Debug().Any("signal", sig).Msg("signal received")
				s.stop <- nil
			} else if runtime.GOOS == "linux" && sig == syscall.Signal(0x17) {
				// Ignore SIGURG; signal 23 (0x17)
			}
		}
	}()
}

func (s *Service) shutdown(sourceErr error) (err error) {
	log.Debug().Msg("function stopping")
	var runtimeErr, instanceErr error

	ctx, cancel := context.WithTimeout(context.Background(), ServerShutdownTimeout)
	defer cancel()
	runtimeErr = s.Shutdown(ctx)

	if i, ok := s.f.(Stopper); ok {
		ctx, cancel = context.WithTimeout(context.Background(), InstanceStopTimeout)
		defer cancel()
		instanceErr = i.Stop(ctx)
	}

	return collapseErrors("shutdown error", sourceErr, instanceErr, runtimeErr)
}

// consumeLoop connects to Kafka and consumes messages, calling the function
// handler for each message.
func (s *Service) consumeLoop(ctx context.Context) error {
	brokers := kafkaBrokers()
	topics := kafkaTopics()
	group := kafkaConsumerGroup()

	if len(brokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS environment variable is required")
	}
	if len(topics) == 0 {
		return fmt.Errorf("KAFKA_TOPICS environment variable is required")
	}
	if group == "" {
		return fmt.Errorf("KAFKA_CONSUMER_GROUP environment variable is required")
	}

	log.Info().
		Strs("brokers", brokers).
		Strs("topics", topics).
		Str("group", group).
		Msg("connecting to kafka")

	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	client, err := sarama.NewConsumerGroup(brokers, group, config)
	if err != nil {
		return fmt.Errorf("creating consumer group: %w", err)
	}
	defer client.Close()

	handler := &consumerGroupHandler{
		f:     s.f,
		ready: &s.ready,
	}

	for {
		if err := client.Consume(ctx, topics, handler); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("consumer error: %w", err)
		}
		if ctx.Err() != nil {
			return nil
		}
		// Rebalance happened; loop to rejoin.
		s.ready.Store(false)
	}
}

// consumerGroupHandler implements sarama.ConsumerGroupHandler.
//
// TODO: support exactly-once semantics via transactional consumer/producer
// TODO: add optional deduplication (e.g. by message key or offset tracking)
type consumerGroupHandler struct {
	f     Handler
	ready *atomic.Bool
}

func (h *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	h.ready.Store(true)
	log.Info().Msg("kafka consumer ready (partitions assigned)")
	return nil
}

func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	h.ready.Store(false)
	log.Info().Msg("kafka consumer partitions revoked")
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		m := Message{
			Key:       msg.Key,
			Value:     msg.Value,
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Timestamp: msg.Timestamp,
		}
		for _, h := range msg.Headers {
			if h != nil {
				m.Headers = append(m.Headers, Header{
					Key:   string(h.Key),
					Value: h.Value,
				})
			}
		}

		// TODO: add retry support (backoff, max attempts, DLQ)
		// TODO: a failed message is effectively lost if a later message in the
		// same partition succeeds, because MarkMessage on a higher offset
		// implicitly commits the earlier one. Consider stopping the partition
		// on error or tracking failed offsets separately.
		if err := h.f.Handle(session.Context(), m); err != nil {
			log.Error().Err(err).
				Str("topic", msg.Topic).
				Int32("partition", msg.Partition).
				Int64("offset", msg.Offset).
				Msg("error handling kafka message")
			continue
		}
		session.MarkMessage(msg, "")
	}
	return nil
}

func kafkaBrokers() []string {
	v := os.Getenv("KAFKA_BROKERS")
	if v == "" {
		return nil
	}
	return strings.Split(v, ",")
}

func kafkaTopics() []string {
	v := os.Getenv("KAFKA_TOPICS")
	if v == "" {
		return nil
	}
	return strings.Split(v, ",")
}

func kafkaConsumerGroup() string {
	return os.Getenv("KAFKA_CONSUMER_GROUP")
}

func listenAddress() string {
	listenAddress := os.Getenv("LISTEN_ADDRESS")
	if listenAddress != "" {
		return listenAddress
	}

	address := os.Getenv("ADDRESS")
	port := os.Getenv("PORT")
	if address != "" || port != "" {
		if address != "" {
			log.Warn().Msg("Environment variable ADDRESS is deprecated and support will be removed in future versions.  Try rebuilding your Function with the latest version of func to use LISTEN_ADDRESS instead.")
		} else {
			address = "127.0.0.1"
		}
		if port != "" {
			log.Warn().Msg("Environment variable PORT is deprecated and support will be removed in future version.s  Try rebuilding your Function with the latest version of func to use LISTEN_ADDRESS instead.")
		} else {
			port = "8080"
		}
		return address + ":" + port
	}

	return DefaultListenAddress
}

func readCfg() (map[string]string, error) {
	cfg := map[string]string{}

	f, err := os.Open("cfg")
	if err != nil {
		log.Debug().Msg("no static config")
		return cfg, nil
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	i := 0
	for scanner.Scan() {
		i++
		line := scanner.Text()
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return cfg, fmt.Errorf("config line %v invalid: %v", i, line)
		}
		cfg[strings.TrimSpace(parts[0])] = strings.Trim(strings.TrimSpace(parts[1]), "\"")
	}
	return cfg, scanner.Err()
}

func newCfg() (cfg map[string]string, err error) {
	if cfg, err = readCfg(); err != nil {
		return
	}

	for _, e := range os.Environ() {
		pair := strings.SplitN(e, "=", 2)
		cfg[pair[0]] = pair[1]
	}
	return
}

func collapseErrors(msg string, ee ...error) (err error) {
	for _, e := range ee {
		if e != nil {
			if err == nil {
				err = e
			} else {
				log.Error().Err(e).Msg(msg)
			}
		}
	}
	return
}
