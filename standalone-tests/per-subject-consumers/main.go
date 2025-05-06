package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Config holds benchmark configuration
type Config struct {
	BatchSize            int
	ConsumeBatchSize     int
	Concurrency          int // Number of concurrent subjects
	NumConsumers         int // Number of consumer goroutines
	NumProducers         int // Number of producer goroutines
	MessageMaxAge        time.Duration
	MessageRate          int // Target message rate per subject
	MessageSize          int // Message size in bytes
	NumDomains           int // Number of JetStream domains to use
	EnableConsumers      bool
	Duration             time.Duration // Benchmark duration
	LatencyFromXSentTime bool
	NatsURL              string // NATS server URL
	NATSUser             string
	NATSPassword         string
	PrintPendingMessages bool
	PubSync              bool
	Replicas             int // Number of replicas
	RetentionPolicy      int
	Storage              int
	ServerShardID        int
	StreamPrefix         string // Stream name prefix
	StreamShards         int    // Number of stream shards
	RequestTimeout       time.Duration
}

func main() {
	// Parse command line flags
	batchSize := flag.Int("batch", 100, "Batch size")
	consumeBatchSize := flag.Int("consumeBatch", 100, "Consume batch size")
	concurrency := flag.Int("concurrency", 100, "Number of concurrent topics")
	numConsumers := flag.Int("consumers", 10, "Number of consumer goroutines")
	numProducers := flag.Int("producers", 10, "Number of producer goroutines")
	messageMaxAge := flag.Duration("msgMaxAge", 0*time.Minute, "Max age of the message to keep in the stream")
	messageRate := flag.Int("rate", 8, "Messages per second per topic")
	messageSize := flag.Int("size", 4096, "Message size in bytes")
	enableConsumers := flag.Bool("enableConsumers", true, "Enables consumers")
	duration := flag.Duration("duration", 5*time.Minute, "Benchmark duration")
	latencyFromXSentTime := flag.Bool("latencyFromXSentTime", false, "Whether to calculate the read latency from X-Sent-Time header")
	natsURL := flag.String("nats", "nats://nats:4222", "NATS server URL")
	natsUser := flag.String("natsUser", "", "NATS server user")
	natsPassword := flag.String("natsPassword", "", "NATS server password")
	numDomains := flag.Int("domains", 3, "Number of JetStream domains to use")
	pubSync := flag.Bool("pubSync", false, "Enables sync publishers")
	pendingMsg := flag.Bool("pendingMsg", false, "Prints pending message counts")
	requestTimeout := flag.Duration("timeout", 60*time.Second, "Request timeout duration")
	replicas := flag.Int("replicas", 3, "Number of stream replicas")
	retentionPolicy := flag.Int("retention", 0, "Stream retention policy; 0 - limit, 1 - workqueue")
	serverShardID := flag.Int("serverShardID", 0, "The server shard ID")
	storage := flag.Int("storage", 0, "Jetstream storage type; 0 - file, 1- memory")
	streamPrefix := flag.String("prefix", "benchmark", "Stream name prefix")
	streamShards := flag.Int("shards", 2, "Number of stream shards")

	flag.Parse()

	// Setup logging
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	// Create benchmark configuration
	config := Config{
		BatchSize:            *batchSize,
		ConsumeBatchSize:     *consumeBatchSize,
		Concurrency:          *concurrency,
		NumConsumers:         *numConsumers,
		NumProducers:         *numProducers,
		MessageMaxAge:        *messageMaxAge,
		MessageRate:          *messageRate,
		MessageSize:          *messageSize,
		EnableConsumers:      *enableConsumers,
		Duration:             *duration,
		LatencyFromXSentTime: *latencyFromXSentTime,
		NatsURL:              *natsURL,
		NATSUser:             *natsUser,
		NATSPassword:         *natsPassword,
		NumDomains:           *numDomains,
		PrintPendingMessages: *pendingMsg,
		PubSync:              *pubSync,
		RequestTimeout:       *requestTimeout,
		Replicas:             *replicas,
		RetentionPolicy:      *retentionPolicy,
		ServerShardID:        *serverShardID,
		Storage:              *storage,
		StreamPrefix:         *streamPrefix,
		StreamShards:         *streamShards,
	}

	// Create benchmark
	benchmark := NewBenchmark(config)

	// Handle interrupt
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	go func() {
		<-signalCh
		log.Warn().Msg("Interrupt received, stopping benchmark")
		close(benchmark.done)
	}()

	// Setup benchmark
	if err := benchmark.Setup(); err != nil {
		log.Fatal().Err(err).Msg("Failed to set up benchmark")
	}
	defer benchmark.Cleanup()

	// Run benchmark
	if err := benchmark.Run(); err != nil {
		log.Fatal().Err(err).Msg("Benchmark failed")
	}

	log.Info().Msg("Benchmark completed successfully")
}

// Benchmark implements the message broker benchmark
type Benchmark struct {
	config           Config
	nc               *nats.Conn
	jsDomains        []jetstream.JetStream
	messagesSent     int64
	messagesReceived int64
	bytesSent        int64
	bytesReceived    int64
	writeLatencies   []time.Duration
	readLatencies    []time.Duration
	latencyMutex     sync.Mutex
	producerWg       sync.WaitGroup
	consumerWg       sync.WaitGroup
	wg               sync.WaitGroup
	done             chan struct{}
	startTime        time.Time
	topicToDomain    map[string]jetstream.JetStream
}

// NewBenchmark creates a new benchmark instance
func NewBenchmark(config Config) *Benchmark {
	return &Benchmark{
		config:         config,
		writeLatencies: make([]time.Duration, 0, 10000),
		readLatencies:  make([]time.Duration, 0, 10000),
		done:           make(chan struct{}),
		jsDomains:      make([]jetstream.JetStream, config.NumDomains),
	}
}

// generateTopicNames creates the list of topics for the benchmark
func (b *Benchmark) generateTopicNames() []string {
	topics := make([]string, b.config.Concurrency)
	for i := 0; i < b.config.Concurrency; i++ {
		shardID := i % b.config.StreamShards
		topics[i] = fmt.Sprintf("%s.shard.%d.topic.%d", b.config.StreamPrefix, shardID, i+b.config.ServerShardID*b.config.Concurrency)
	}
	return topics
}

func (b *Benchmark) buildTopicToDomainMap(topics []string) {
	b.topicToDomain = make(map[string]jetstream.JetStream, len(topics))
	for _, topic := range topics {
		var shardID int
		_, err := fmt.Sscanf(topic, b.config.StreamPrefix+".shard.%d.", &shardID)
		if err != nil {
			log.Fatal().Err(err).Str("topic", topic).Msg("Failed to parse shard ID from topic name")
		}

		domainID := shardID % b.config.NumDomains
		if domainID >= len(b.jsDomains) {
			log.Fatal().
				Int("domain_id", domainID).
				Int("total_domains", len(b.jsDomains)).
				Msg("Domain ID exceeds available JetStream domains")
		}

		b.topicToDomain[topic] = b.jsDomains[domainID]
	}
}

// Setup prepares the benchmark environment
func (b *Benchmark) Setup() error {
	// Connect to NATS
	opts := []nats.Option{
		nats.Name("JetStream Benchmark"),
		nats.ReconnectWait(5 * time.Second),
		nats.MaxReconnects(10),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			// Logger access from config.go file
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			// Logger access from config.go file
		}),
		nats.ErrorHandler(func(nc *nats.Conn, s *nats.Subscription, err error) {
			// Logger access from config.go file
		}),
		nats.UserInfo(b.config.NATSUser, b.config.NATSPassword),
	}

	nc, err := nats.Connect(b.config.NatsURL, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}

	b.nc = nc

	for i := 0; i < b.config.NumDomains; i++ {
		domainName := fmt.Sprintf("c%d", i)
		js, err := jetstream.NewWithDomain(nc, domainName)
		if err != nil {
			return fmt.Errorf("failed to create JetStream for domain %s: %w", domainName, err)
		}
		b.jsDomains[i] = js
	}

	// Create stream shards with wildcard subjects
	if err := b.createStreamShards(); err != nil {
		return fmt.Errorf("failed to create stream shards: %w", err)
	}
	return nil
}

// createStreamShards creates the required JetStream streams with wildcard subjects
func (b *Benchmark) createStreamShards() error {
	for i := 0; i < b.config.StreamShards; i++ {
		streamName := fmt.Sprintf("%s-%d", b.config.StreamPrefix, i)

		// Use wildcard subject pattern for each shard
		subjectPattern := fmt.Sprintf("%s.shard.%d.>", b.config.StreamPrefix, i)

		jsCfg := jetstream.StreamConfig{
			Name:      streamName,
			MaxAge:    b.config.MessageMaxAge,
			Replicas:  b.config.Replicas,
			Retention: jetstream.RetentionPolicy(b.config.RetentionPolicy),
			Storage:   jetstream.StorageType(b.config.Storage),
			Subjects:  []string{subjectPattern},
		}

		js := b.jsDomains[i%b.config.NumDomains]
		_, err := js.CreateStream(context.Background(), jsCfg)

		if err != nil {
			return fmt.Errorf("failed to create stream %s: %w", streamName, err)
		}
	}

	return nil
}

// Cleanup releases resources
func (b *Benchmark) Cleanup() {
	if b.nc != nil {
		b.nc.Close()
	}
}

// Run executes the benchmark
func (b *Benchmark) Run() error {
	// Generate topic names
	topicNames := b.generateTopicNames()

	b.buildTopicToDomainMap(topicNames)

	domainTopicCounts := make(map[string]int)

	for topic, js := range b.topicToDomain {
		domain := js.Options().Domain
		domainTopicCounts[domain]++

		log.Debug().
			Str("topic", topic).
			Str("domain", domain).
			Msg("Mapped topic to domain")
	}

	for domain, count := range domainTopicCounts {
		log.Info().
			Str("domain", domain).
			Int("topic_count", count).
			Msg("Total topics for domain")
	}

	// Log benchmark parameters
	log.Info().
		Int("concurrency", b.config.Concurrency).
		Int("consumers", b.config.NumConsumers).
		Int("producers", b.config.NumProducers).
		Dur("message_max_age", b.config.MessageMaxAge).
		Int("message_rate", b.config.MessageRate).
		Int("message_size", b.config.MessageSize).
		Int("stream_shards", b.config.StreamShards).
		Int("storage", b.config.Storage).
		Int("replicas", b.config.Replicas).
		Dur("duration", b.config.Duration).
		Bool("enable_consumers", b.config.EnableConsumers).
		Bool("pub_sync", b.config.PubSync).
		Msg("Starting benchmark")

	// Start the benchmark
	b.startTime = time.Now()

	// Start stats reporter
	b.startStatsReporter()

	// Start consumers (one per topic)

	if b.config.EnableConsumers {
		b.startConsumers(topicNames)
	}

	// Start producers
	b.startProducers(topicNames)

	// Create a timer for the benchmark duration
	benchmarkTimer := time.NewTimer(b.config.Duration)

	// Wait for benchmark to complete or be interrupted
	select {
	case <-benchmarkTimer.C:
		log.Info().Msg("Benchmark duration reached")
	case <-b.done:
		log.Info().Msg("Benchmark interrupted")
		if !benchmarkTimer.Stop() {
			<-benchmarkTimer.C
		}
	}

	// Signal all goroutines to stop
	close(b.done)

	// Wait for producers and consumers to finish
	log.Info().Msg("Waiting for producers to finish...")
	b.producerWg.Wait()

	log.Info().Msg("Waiting for consumers to finish...")
	b.consumerWg.Wait()

	log.Info().Msg("Waiting for other goroutines...")
	b.wg.Wait()

	// Report final results
	b.reportResults()

	return nil
}

// startStatsReporter starts a goroutine that periodically reports benchmark stats
func (b *Benchmark) startStatsReporter() {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()

		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		lastMsgsSent := int64(0)
		lastMsgsReceived := int64(0)
		lastTime := time.Now()

		for {
			select {
			case <-ticker.C:
				now := time.Now()
				elapsed := now.Sub(lastTime)

				currentSent := atomic.LoadInt64(&b.messagesSent)
				currentReceived := atomic.LoadInt64(&b.messagesReceived)

				sendRate := float64(currentSent-lastMsgsSent) / elapsed.Seconds()
				receiveRate := float64(currentReceived-lastMsgsReceived) / elapsed.Seconds()

				perTopicSendRate := sendRate / float64(b.config.Concurrency)
				perTopicReceiveRate := receiveRate / float64(b.config.Concurrency)

				// Calculate and report current latency stats
				b.latencyMutex.Lock()
				writeSampleCount := len(b.writeLatencies)
				readSampleCount := len(b.readLatencies)
				b.latencyMutex.Unlock()

				pendingMsgCount := 0
				if b.config.PrintPendingMessages {
					for i, js := range b.jsDomains {
						count := js.PublishAsyncPending()
						log.Debug().Int("domain", i).Int("pending", count).Msg("Pending messages in domain")
						pendingMsgCount += count
					}
				}

				log.Info().
					Int64("total_sent", currentSent).
					Int64("total_received", currentReceived).
					Float64("msgs_per_sec", sendRate).
					Float64("receive_rate", receiveRate).
					Int("replicas", b.config.Replicas).
					Float64("per_topic_send_rate", perTopicSendRate).
					Float64("per_topic_receive_rate", perTopicReceiveRate).
					Int("write_latency_samples", writeSampleCount).
					Int("read_latency_samples", readSampleCount).
					Int("pending_msg_count", pendingMsgCount).
					Msg("Benchmark progress")

				lastMsgsSent = currentSent
				lastMsgsReceived = currentReceived
				lastTime = now

			case <-b.done:
				return
			}
		}
	}()
}

func (b *Benchmark) startConsumers(topicNames []string) {
	// If no consumers specified, use one per shard (default behavior)
	if b.config.NumConsumers <= 0 {
		b.config.NumConsumers = b.config.StreamShards
	}

	// Group topics by shard first
	shardToTopics := make(map[int][]string)
	for i, topic := range topicNames {
		shardID := i % b.config.StreamShards
		shardToTopics[shardID] = append(shardToTopics[shardID], topic)
	}

	// Calculate how many consumers per shard
	consumersPerShard := make(map[int]int)

	// Distribute consumers evenly across shards
	baseConsumersPerShard := b.config.NumConsumers / b.config.StreamShards
	extraConsumers := b.config.NumConsumers % b.config.StreamShards
	for shardID := 0; shardID < b.config.StreamShards; shardID++ {
		if shardID < extraConsumers {
			consumersPerShard[shardID] = baseConsumersPerShard + 1
		} else {
			consumersPerShard[shardID] = baseConsumersPerShard
		}
	}

	log.Debug().
		Int("total_consumers", b.config.NumConsumers).
		Int("shards", b.config.StreamShards).
		Interface("consumers_per_shard", consumersPerShard).
		Msg("Distributing consumers across shards")

	// Start consumers for each shard
	consumerCounter := 0
	for shardID, topics := range shardToTopics {
		numConsumersForShard := consumersPerShard[shardID]
		if numConsumersForShard <= 0 {
			continue
		}

		// Calculate topics per consumer for this shard
		topicsPerConsumer := (len(topics) + numConsumersForShard - 1) / numConsumersForShard
		for c := 0; c < numConsumersForShard; c++ {
			b.consumerWg.Add(1)

			// Calculate topic range for this consumer within the shard
			startIdx := c * topicsPerConsumer
			endIdx := (c + 1) * topicsPerConsumer
			if endIdx > len(topics) {
				endIdx = len(topics)
			}

			// Skip if no topics assigned to this consumer
			if startIdx >= len(topics) {
				b.consumerWg.Done()
				continue
			}

			// Get topics for this consumer within this shard
			consumerTopics := topics[startIdx:endIdx]
			if len(consumerTopics) == 0 {
				b.consumerWg.Done()
				continue
			}

			globalConsumerID := consumerCounter
			consumerCounter++

			go func(consumerID int, shardID int, consumerTopics []string) {
				defer b.consumerWg.Done()
				streamName := fmt.Sprintf("%s-%d", b.config.StreamPrefix, shardID)
				log.Debug().
					Int("consumer_id", consumerID).
					Int("shard_id", shardID).
					Int("topics_count", len(consumerTopics)).
					Strs("topics", consumerTopics).
					Msg("Creating stream consumer")

				// Create an ephemeral pull consumer with a filter subject array
				consumerConfig := jetstream.ConsumerConfig{
					AckPolicy:     jetstream.AckExplicitPolicy,
					AckWait:       30 * time.Second,
					MaxAckPending: 10000,
					DeliverPolicy: jetstream.DeliverAllPolicy,
					//	DeliverPolicy:  jetstream.DeliverLastPerSubjectPolicy,
					FilterSubjects: consumerTopics, // Use assigned topics for this consumer
					MaxDeliver:     1,              // Only deliver once
					ReplayPolicy:   jetstream.ReplayInstantPolicy,
					// No Durable name for ephemeral consumer
				}

				ctx, cancel := context.WithTimeout(context.Background(), b.config.RequestTimeout)

				jsWithDomain := b.jsDomains[shardID%b.config.NumDomains]

				consumer, err := jsWithDomain.CreateOrUpdateConsumer(
					ctx,
					streamName,
					consumerConfig,
				)
				cancel()

				if err != nil {
					log.Fatal().Err(err).
						Int("consumer_id", consumerID).
						Int("shard_id", shardID).
						Int("topic_count", len(consumerTopics)).
						Msg("Failed to create consumer")
				}

				log.Debug().
					Int("consumer_id", consumerID).
					Int("shard_id", shardID).
					Int("topic_count", len(consumerTopics)).
					Msg("Stream consumer created")

				// Process messages until benchmark is done
				for {
					select {
					case <-b.done:
						return
					default:
						// Fetch messages in batches for efficiency
						msgs, err := consumer.Fetch(b.config.ConsumeBatchSize)
						if err != nil {
							if err != context.DeadlineExceeded && err != nats.ErrTimeout {
								log.Error().
									Err(err).
									Int("consumer_id", consumerID).
									Int("shard_id", shardID).
									Msg("Error fetching messages")
							}
							time.Sleep(10 * time.Millisecond) // Small sleep to prevent CPU spinning
							continue
						}

						// Process messages
						for msg := range msgs.Messages() {
							// Record message reception
							atomic.AddInt64(&b.messagesReceived, 1)
							atomic.AddInt64(&b.bytesReceived, int64(len(msg.Data())))

							// Calculate latency using message metadata
							meta, err := msg.Metadata()
							if err == nil && meta != nil {
								// Calculate latency using the message timestamp
								readLatency := time.Since(meta.Timestamp)
								b.recordReadLatency(readLatency)
							} else if sentTimeStr := msg.Headers().Get("X-Sent-Time"); sentTimeStr != "" && b.config.LatencyFromXSentTime {
								if sentTime, err := time.Parse(time.RFC3339Nano, sentTimeStr); err == nil {
									readLatency := time.Since(sentTime)
									b.recordReadLatency(readLatency)

									log.Warn().
										Dur("read_latency", readLatency).
										Str("subject", msg.Subject()).
										Int("consumer_id", consumerID).
										Msg("Calculated read latency from X-Sent-Time header")
								} else {
									log.Warn().
										Err(err).
										Str("subject", msg.Subject()).
										Int("consumer_id", consumerID).
										Msg("Failed to parse sent time from X-Sent-Time header")
								}
							}

							// Acknowledge message
							if err := msg.Ack(); err != nil {
								log.Warn().
									Err(err).
									Str("subject", msg.Subject()).
									Int("consumer_id", consumerID).
									Msg("Failed to acknowledge message")
							}
						}
					}
				}
			}(globalConsumerID, shardID, consumerTopics)
		}
	}
}

// startProducers starts producer goroutines
func (b *Benchmark) startProducers(topicNames []string) {
	msgData := make([]byte, b.config.MessageSize)
	for i := range msgData {
		msgData[i] = byte(i % 256)
	}

	topicsPerProducer := (b.config.Concurrency + b.config.NumProducers - 1) / b.config.NumProducers

	for p := 0; p < b.config.NumProducers; p++ {
		b.producerWg.Add(1)
		go func(producerID int) {
			defer b.producerWg.Done()

			startIdx := producerID * topicsPerProducer
			endIdx := (producerID + 1) * topicsPerProducer
			if endIdx > b.config.Concurrency {
				endIdx = b.config.Concurrency
			}
			if startIdx >= len(topicNames) {
				return
			}

			producerTopics := topicNames[startIdx:endIdx]

			log.Debug().
				Int("producer_id", producerID).
				Int("topics", len(producerTopics)).
				Msg("Producer started")

			tickers := make([]*time.Ticker, len(producerTopics))
			for i := range producerTopics {
				interval := time.Second / time.Duration(b.config.MessageRate)
				tickers[i] = time.NewTicker(interval)
			}
			defer func() {
				for _, t := range tickers {
					t.Stop()
				}
			}()

			type asyncPublishTracker struct {
				sendTime  time.Time
				topic     string
				future    jetstream.PubAckFuture
				processed bool
			}
			var pendingPublishes []asyncPublishTracker
			currentBatchSize := 0

			for {
				select {
				case <-b.done:
					if !b.config.PubSync && len(pendingPublishes) > 0 {
						unprocessed := map[jetstream.JetStream][]*asyncPublishTracker{}
						for i := range pendingPublishes {
							if !pendingPublishes[i].processed {
								js := b.topicToDomain[pendingPublishes[i].topic]
								unprocessed[js] = append(unprocessed[js], &pendingPublishes[i])
							}
						}

						for js, list := range unprocessed {
							ctx, cancel := context.WithTimeout(context.Background(), b.config.RequestTimeout)
							select {
							case <-js.PublishAsyncComplete():
								for _, pub := range list {
									select {
									case <-pub.future.Ok():
										b.recordWriteLatency(time.Since(pub.sendTime))
										atomic.AddInt64(&b.messagesSent, 1)
										atomic.AddInt64(&b.bytesSent, int64(b.config.MessageSize))
										pub.processed = true
									case err := <-pub.future.Err():
										log.Error().Err(err).Str("topic", pub.topic).Msg("Final async publish error")
										pub.processed = true
									default:
										log.Warn().Str("topic", pub.topic).Msg("Future not ready despite completion")
									}
								}
							case <-ctx.Done():
								log.Warn().Int("producer_id", producerID).Msg("Timeout waiting for async publish completion")
							}
							cancel()
						}
					}
					return

				default:
					published := false
					for i, topic := range producerTopics {
						select {
						case <-tickers[i].C:
							published = true
							sendTime := time.Now()
							msg := &nats.Msg{
								Subject: topic,
								Header:  nats.Header{"X-Sent-Time": []string{sendTime.Format(time.RFC3339Nano)}},
								Data:    msgData,
							}

							js := b.topicToDomain[topic]

							if b.config.PubSync {
								ctx, cancel := context.WithTimeout(context.Background(), b.config.RequestTimeout)
								_, err := js.PublishMsg(ctx, msg)
								cancel()
								if err != nil {
									log.Error().Err(err).Str("topic", topic).Msg("Sync publish failed")
								} else {
									b.recordWriteLatency(time.Since(sendTime))
									atomic.AddInt64(&b.messagesSent, 1)
									atomic.AddInt64(&b.bytesSent, int64(b.config.MessageSize))
								}
							} else {
								future, err := js.PublishMsgAsync(msg)
								if err != nil {
									log.Error().Err(err).Str("topic", topic).Msg("Async publish failed")
								} else {
									pendingPublishes = append(pendingPublishes, asyncPublishTracker{
										sendTime: sendTime, topic: topic, future: future,
									})
									currentBatchSize++
								}
							}
						default:
						}
					}

					if !b.config.PubSync && currentBatchSize >= b.config.BatchSize {
						for i := range pendingPublishes {
							if pendingPublishes[i].processed {
								continue
							}
							select {
							case <-pendingPublishes[i].future.Ok():
								b.recordWriteLatency(time.Since(pendingPublishes[i].sendTime))
								atomic.AddInt64(&b.messagesSent, 1)
								atomic.AddInt64(&b.bytesSent, int64(b.config.MessageSize))
								pendingPublishes[i].processed = true
							case err := <-pendingPublishes[i].future.Err():
								log.Error().Err(err).Str("topic", pendingPublishes[i].topic).Msg("Async publish error")
								pendingPublishes[i].processed = true
							default:
							}
						}
						pendingPublishes = pendingPublishes[:0]
						currentBatchSize = 0
					}

					if !published {
						runtime.Gosched()
					}
				}
			}
		}(p)
	}
}

// formatDuration formats a duration in milliseconds with 3 decimal places
func formatDuration(d time.Duration) string {
	return fmt.Sprintf("%.3f ms", float64(d)/float64(time.Millisecond))
}

// calculateQuantiles calculates p50, p90, p95, and p99 quantiles
func calculateQuantiles(durations []time.Duration) map[string]time.Duration {
	if len(durations) == 0 {
		return map[string]time.Duration{
			"p50": 0,
			"p90": 0,
			"p95": 0,
			"p99": 0,
		}
	}

	// Sort the durations
	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})

	// Calculate quantile indices
	p50Idx := int(float64(len(durations)) * 0.5)
	p90Idx := int(float64(len(durations)) * 0.9)
	p95Idx := int(float64(len(durations)) * 0.95)
	p99Idx := int(float64(len(durations)) * 0.99)

	// Bound check
	if p50Idx >= len(durations) {
		p50Idx = len(durations) - 1
	}
	if p90Idx >= len(durations) {
		p90Idx = len(durations) - 1
	}
	if p95Idx >= len(durations) {
		p95Idx = len(durations) - 1
	}
	if p99Idx >= len(durations) {
		p99Idx = len(durations) - 1
	}

	return map[string]time.Duration{
		"p50": durations[p50Idx],
		"p90": durations[p90Idx],
		"p95": durations[p95Idx],
		"p99": durations[p99Idx],
	}
}

// reportResults calculates and reports the final benchmark results
func (b *Benchmark) reportResults() {
	duration := time.Since(b.startTime)
	msgsSent := atomic.LoadInt64(&b.messagesSent)
	msgsReceived := atomic.LoadInt64(&b.messagesReceived)
	bytesSent := atomic.LoadInt64(&b.bytesSent)
	bytesReceived := atomic.LoadInt64(&b.bytesReceived)

	// Calculate message rates
	sendRate := float64(msgsSent) / duration.Seconds()
	receiveRate := float64(msgsReceived) / duration.Seconds()
	perTopicSendRate := sendRate / float64(b.config.Concurrency)
	perTopicReceiveRate := receiveRate / float64(b.config.Concurrency)

	// Report overall performance
	log.Info().
		Int("concurrency", b.config.Concurrency).
		Int("consumers", b.config.NumConsumers).
		Int("producers", b.config.NumProducers).
		Int("replicas", b.config.Replicas).
		Dur("message_max_age", b.config.MessageMaxAge).
		Int64("messages_sent", msgsSent).
		Int64("messages_received", msgsReceived).
		Int64("bytes_sent", bytesSent).
		Int64("bytes_received", bytesReceived).
		Float64("msgs_per_sec", sendRate).
		Float64("receive_per_sec", receiveRate).
		Float64("per_topic_send_rate", perTopicSendRate).
		Float64("per_topic_receive_rate", perTopicReceiveRate).
		Int("stream_shards", b.config.StreamShards).
		Dur("duration", duration).
		Msg("Benchmark Complete")

	// Calculate latency statistics
	b.latencyMutex.Lock()
	writeLatenciesCopy := make([]time.Duration, len(b.writeLatencies))
	copy(writeLatenciesCopy, b.writeLatencies)

	readLatenciesCopy := make([]time.Duration, len(b.readLatencies))
	copy(readLatenciesCopy, b.readLatencies)
	b.latencyMutex.Unlock()

	// Calculate write latency stats
	if len(writeLatenciesCopy) > 0 {
		var minWrite, maxWrite, totalWrite time.Duration = writeLatenciesCopy[0], writeLatenciesCopy[0], 0
		for _, d := range writeLatenciesCopy {
			totalWrite += d
			if d < minWrite {
				minWrite = d
			}
			if d > maxWrite {
				maxWrite = d
			}
		}
		avgWrite := totalWrite / time.Duration(len(writeLatenciesCopy))
		writeQuantiles := calculateQuantiles(writeLatenciesCopy)

		log.Info().
			Int("samples", len(writeLatenciesCopy)).
			Str("min", formatDuration(minWrite)).
			Str("max", formatDuration(maxWrite)).
			Str("avg", formatDuration(avgWrite)).
			Str("p50", formatDuration(writeQuantiles["p50"])).
			Str("p90", formatDuration(writeQuantiles["p90"])).
			Str("p95", formatDuration(writeQuantiles["p95"])).
			Str("p99", formatDuration(writeQuantiles["p99"])).
			Msg("Write Latency Statistics (ms)")
	} else {
		log.Info().Msg("No write latency samples collected")
	}

	// Calculate read latency stats
	if len(readLatenciesCopy) > 0 {
		var minRead, maxRead, totalRead time.Duration = readLatenciesCopy[0], readLatenciesCopy[0], 0
		for _, d := range readLatenciesCopy {
			totalRead += d
			if d < minRead {
				minRead = d
			}
			if d > maxRead {
				maxRead = d
			}
		}
		avgRead := totalRead / time.Duration(len(readLatenciesCopy))
		readQuantiles := calculateQuantiles(readLatenciesCopy)

		log.Info().
			Int("samples", len(readLatenciesCopy)).
			Str("min", formatDuration(minRead)).
			Str("max", formatDuration(maxRead)).
			Str("avg", formatDuration(avgRead)).
			Str("p50", formatDuration(readQuantiles["p50"])).
			Str("p90", formatDuration(readQuantiles["p90"])).
			Str("p95", formatDuration(readQuantiles["p95"])).
			Str("p99", formatDuration(readQuantiles["p99"])).
			Msg("Read Latency Statistics (ms)")
	} else {
		log.Info().Msg("No read latency samples collected")
	}
}

func (b *Benchmark) recordReadLatency(latency time.Duration) {
	b.latencyMutex.Lock()
	b.readLatencies = append(b.readLatencies, latency)
	b.latencyMutex.Unlock()
}

func (b *Benchmark) recordWriteLatency(latency time.Duration) {
	b.latencyMutex.Lock()
	b.writeLatencies = append(b.writeLatencies, latency)
	b.latencyMutex.Unlock()
}
