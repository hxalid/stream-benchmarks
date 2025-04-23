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
	StreamPrefix         string // Stream name prefix
	StreamShards         int    // Number of stream shards
	RequestTimeout       time.Duration
	UseRepublish         bool // Whether to use republish instead of JetStream consumers
}

func main() {
	// Parse command line flags
	batchSize := flag.Int("batch", 100, "Batch size")
	consumeBatchSize := flag.Int("consumeBatch", 200, "Consume batch size")
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
	natsUser := flag.String("natsUser", "benchmark", "NATS server user")
	natsPassword := flag.String("natsPassword", "benchmarkbenchmarkbenchmark", "NATS server password")
	pubSync := flag.Bool("pubSync", false, "Enables sync publishers")
	pendingMsg := flag.Bool("pendingMsg", true, "Prints pending message counts")
	requestTimeout := flag.Duration("timeout", 60*time.Second, "Request timeout duration")
	replicas := flag.Int("replicas", 3, "Number of stream replicas")
	retentionPolicy := flag.Int("retention", 0, "Stream retention policy; 0 - limit, 1 - workqueue")
	storage := flag.Int("storage", 0, "Jetstream storage type; 0 - file, 1- memory")
	streamPrefix := flag.String("prefix", "benchmark", "Stream name prefix")
	streamShards := flag.Int("shards", 2, "Number of stream shards")
	useRepublish := flag.Bool("useRepublish", false, "Use republish with core NATS subscribers instead of JetStream consumers")

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
		PrintPendingMessages: *pendingMsg,
		PubSync:              *pubSync,
		RequestTimeout:       *requestTimeout,
		Replicas:             *replicas,
		RetentionPolicy:      *retentionPolicy,
		Storage:              *storage,
		StreamPrefix:         *streamPrefix,
		StreamShards:         *streamShards,
		UseRepublish:         *useRepublish,
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
	js               jetstream.JetStream
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
	subscriptions    []*nats.Subscription // For core NATS subscriptions
	subMutex         sync.Mutex           // Mutex for subscriptions
}

// NewBenchmark creates a new benchmark instance
func NewBenchmark(config Config) *Benchmark {
	return &Benchmark{
		config:         config,
		writeLatencies: make([]time.Duration, 0, 10000),
		readLatencies:  make([]time.Duration, 0, 10000),
		subscriptions:  make([]*nats.Subscription, 0),
		done:           make(chan struct{}),
	}
}

// generateTopicNames creates the list of topics for the benchmark
func (b *Benchmark) generateTopicNames() []string {
	topics := make([]string, b.config.Concurrency)
	for i := 0; i < b.config.Concurrency; i++ {
		shardID := i % b.config.StreamShards
		topics[i] = fmt.Sprintf("%s.shard.%d.topic.%d", b.config.StreamPrefix, shardID, i)
	}
	return topics
}

func (b *Benchmark) Setup() error {
	// Connect to NATS
	opts := []nats.Option{
		nats.Name("JetStream Benchmark"),
		nats.ReconnectWait(5 * time.Second),
		nats.MaxReconnects(10),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Warn().Err(err).Msg("NATS disconnected")
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Info().Str("url", nc.ConnectedUrl()).Msg("NATS reconnected")
		}),
		nats.ErrorHandler(func(nc *nats.Conn, s *nats.Subscription, err error) {
			log.Error().Err(err).Msg("NATS error")
		}),
		nats.UserInfo(b.config.NATSUser, b.config.NATSPassword),
		// Set a larger buffer size for high concurrency
		nats.ReconnectBufSize(128 * 1024 * 1024), // 128MB reconnect buffer
	}

	nc, err := nats.Connect(b.config.NatsURL, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}

	b.nc = nc

	newJS, err := jetstream.New(
		nc,
		jetstream.WithPublishAsyncMaxPending(1024),
	)
	if err != nil {
		return fmt.Errorf("failed to create a Jetstream instance: %w", err)
	}

	b.js = newJS

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

		// Republish configuration if using core NATS subscribers
		var republish *jetstream.RePublish
		if b.config.UseRepublish {
			republish = &jetstream.RePublish{
				Source:      subjectPattern,                                                     // Original subjects
				Destination: fmt.Sprintf("republished.%s.shard.%d.>", b.config.StreamPrefix, i), // Republished destination
			}
			log.Info().
				Str("stream", streamName).
				Str("source", subjectPattern).
				Str("destination", republish.Destination).
				Msg("Configuring stream with republish")
		}

		jsCfg := jetstream.StreamConfig{
			Name:      streamName,
			MaxAge:    b.config.MessageMaxAge,
			Replicas:  b.config.Replicas,
			Retention: jetstream.RetentionPolicy(b.config.RetentionPolicy),
			Storage:   jetstream.StorageType(b.config.Storage),
			Subjects:  []string{subjectPattern},
			RePublish: republish,
		}

		// Create or update stream
		_, err := b.js.CreateStream(context.Background(), jsCfg)
		if err != nil {
			return fmt.Errorf("failed to create stream %s: %w", streamName, err)
		}

		log.Info().
			Str("stream", streamName).
			Str("subject_pattern", subjectPattern).
			Bool("with_republish", b.config.UseRepublish).
			Msg("Stream created successfully")
	}

	return nil
}

// Cleanup releases resources
func (b *Benchmark) Cleanup() {
	// Clean up core NATS subscriptions if we're using republish
	if b.config.UseRepublish {
		b.cleanupSubscriptions()
	}

	if b.nc != nil {
		b.nc.Close()
	}
}

// cleanupSubscriptions unsubscribes from all subscriptions
func (b *Benchmark) cleanupSubscriptions() {
	b.subMutex.Lock()
	defer b.subMutex.Unlock()

	for _, sub := range b.subscriptions {
		if sub != nil {
			sub.Unsubscribe()
		}
	}

	log.Info().Int("count", len(b.subscriptions)).Msg("Cleaned up subscriptions")
	b.subscriptions = nil
}

// Run executes the benchmark
func (b *Benchmark) Run() error {
	// Generate topic names
	topicNames := b.generateTopicNames()

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
		Bool("use_republish", b.config.UseRepublish).
		Msg("Starting benchmark")

	// Start the benchmark
	b.startTime = time.Now()

	// Start stats reporter
	b.startStatsReporter()

	// Start consumers (one per topic)
	if b.config.EnableConsumers {
		if b.config.UseRepublish {
			// Start core NATS subscribers with republish
			b.startCoreSubscribers(topicNames)
		} else {
			// Start JetStream consumers (original approach)
			b.startConsumers(topicNames)
		}
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

				// Process write latencies if we have samples
				var p50WriteLatency, p95WriteLatency, p99WriteLatency time.Duration
				if writeSampleCount > 0 {
					// Create a copy to avoid modifying the original slice while sorting
					wLatencies := make([]time.Duration, writeSampleCount)
					copy(wLatencies, b.writeLatencies)

					// Sort for percentile calculation
					sort.Slice(wLatencies, func(i, j int) bool {
						return wLatencies[i] < wLatencies[j]
					})

					// Calculate percentiles with safe index access
					p50WriteLatency = b.getPercentile(wLatencies, 50)
					p95WriteLatency = b.getPercentile(wLatencies, 95)
					p99WriteLatency = b.getPercentile(wLatencies, 99)
				}

				// Process read latencies if we have samples
				var p50ReadLatency, p95ReadLatency, p99ReadLatency time.Duration
				if readSampleCount > 0 {
					// Create a copy to avoid modifying the original slice while sorting
					rLatencies := make([]time.Duration, readSampleCount)
					copy(rLatencies, b.readLatencies)

					// Sort for percentile calculation
					sort.Slice(rLatencies, func(i, j int) bool {
						return rLatencies[i] < rLatencies[j]
					})

					// Calculate percentiles with safe index access
					p50ReadLatency = b.getPercentile(rLatencies, 50)
					p95ReadLatency = b.getPercentile(rLatencies, 95)
					p99ReadLatency = b.getPercentile(rLatencies, 99)
				}

				// Reset latency arrays periodically to prevent excessive memory usage
				if writeSampleCount > 100000 {
					b.writeLatencies = b.writeLatencies[writeSampleCount-50000:]
				}
				if readSampleCount > 100000 {
					b.readLatencies = b.readLatencies[readSampleCount-50000:]
				}
				b.latencyMutex.Unlock()

				pendingMsgCount := 0
				if b.config.PrintPendingMessages {
					pendingMsgCount = b.js.PublishAsyncPending()
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
					Float64("write_p50_ms", float64(p50WriteLatency)/float64(time.Millisecond)).
					Float64("write_p95_ms", float64(p95WriteLatency)/float64(time.Millisecond)).
					Float64("write_p99_ms", float64(p99WriteLatency)/float64(time.Millisecond)).
					Float64("read_p50_ms", float64(p50ReadLatency)/float64(time.Millisecond)).
					Float64("read_p95_ms", float64(p95ReadLatency)/float64(time.Millisecond)).
					Float64("read_p99_ms", float64(p99ReadLatency)/float64(time.Millisecond)).
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

// getPercentile safely gets a percentile value from a sorted duration slice
func (b *Benchmark) getPercentile(sortedDurations []time.Duration, percentile int) time.Duration {
	if len(sortedDurations) == 0 {
		return 0
	}

	// Calculate index for the given percentile
	idx := int(float64(len(sortedDurations)) * float64(percentile) / 100.0)

	// Ensure index is within bounds
	if idx >= len(sortedDurations) {
		idx = len(sortedDurations) - 1
	}
	if idx < 0 {
		idx = 0
	}

	return sortedDurations[idx]
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
					AckPolicy:      jetstream.AckExplicitPolicy,
					AckWait:        30 * time.Second,
					MaxAckPending:  10000,
					DeliverPolicy:  jetstream.DeliverAllPolicy,
					FilterSubjects: consumerTopics, // Use assigned topics for this consumer
					MaxDeliver:     1,              // Only deliver once
					ReplayPolicy:   jetstream.ReplayInstantPolicy,
					// No Durable name for ephemeral consumer
				}

				ctx, cancel := context.WithTimeout(context.Background(), b.config.RequestTimeout)
				consumer, err := b.js.CreateOrUpdateConsumer(
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

// Core functions for subscribers and producers
// Core functions for subscribers and producers

// startCoreSubscribers sets up core NATS subscriptions for the republished subjects
func (b *Benchmark) startCoreSubscribers(topicNames []string) {
	// Calculate subjects per subscriber
	totalSubjects := len(topicNames)
	subjectsPerSubscriber := (totalSubjects + b.config.NumConsumers - 1) / b.config.NumConsumers

	// Start subscribers
	for c := 0; c < b.config.NumConsumers; c++ {
		b.consumerWg.Add(1)
		go func(subscriberID int) {
			defer b.consumerWg.Done()

			// Calculate subject range for this subscriber
			startIdx := subscriberID * subjectsPerSubscriber
			endIdx := (startIdx + subjectsPerSubscriber)
			if endIdx > totalSubjects {
				endIdx = totalSubjects
			}

			// Skip if no subjects assigned
			if startIdx >= totalSubjects {
				log.Debug().Int("subscriber_id", subscriberID).Msg("Subscriber has no subjects assigned")
				return
			}

			log.Debug().
				Int("subscriber_id", subscriberID).
				Int("start_idx", startIdx).
				Int("end_idx", endIdx).
				Msg("Subscriber handling republished subjects")

			// Subscribe to all subjects in range
			for subjectIdx := startIdx; subjectIdx < endIdx; subjectIdx++ {
				originalSubject := topicNames[subjectIdx]
				// Calculate the republished subject pattern
				// Convert from "prefix.shard.N.topic.M" to "republished.prefix.shard.N.topic.M"
				republishedSubject := fmt.Sprintf("republished.%s", originalSubject)

				// Create core NATS subscription
				sub, err := b.nc.Subscribe(republishedSubject, func(msg *nats.Msg) {
					// Get sent time from header if available
					if msg.Header != nil {
						if sentTimeStr := msg.Header.Get("X-Sent-Time"); sentTimeStr != "" {
							if sentTime, err := time.Parse(time.RFC3339Nano, sentTimeStr); err == nil {
								readLatency := time.Since(sentTime)
								b.recordReadLatency(readLatency)
							}
						}
					}

					// Record message reception
					atomic.AddInt64(&b.messagesReceived, 1)
					atomic.AddInt64(&b.bytesReceived, int64(len(msg.Data)))
				})

				if err != nil {
					log.Error().
						Err(err).
						Int("subscriber_id", subscriberID).
						Str("subject", republishedSubject).
						Msg("Error subscribing to subject")
					continue
				}

				// Store subscription for cleanup
				b.subMutex.Lock()
				b.subscriptions = append(b.subscriptions, sub)
				b.subMutex.Unlock()
			}

			log.Debug().
				Int("subscriber_id", subscriberID).
				Int("subject_count", endIdx-startIdx).
				Msg("Created core NATS subscriptions")

			// Wait until done
			<-b.done
		}(c)
	}
}

func (b *Benchmark) startProducers(topicNames []string) {
	// Prepare message data
	msgData := make([]byte, b.config.MessageSize)
	for i := range msgData {
		msgData[i] = byte(i % 256)
	}

	// Calculate how many topics each producer handles
	topicsPerProducer := (len(topicNames) + b.config.NumProducers - 1) / b.config.NumProducers

	// Start producer goroutines
	for p := 0; p < b.config.NumProducers; p++ {
		b.producerWg.Add(1)
		go func(producerID int) {
			defer b.producerWg.Done()

			// Calculate topic range for this producer
			startIdx := producerID * topicsPerProducer
			endIdx := (producerID + 1) * topicsPerProducer
			if endIdx > len(topicNames) {
				endIdx = len(topicNames)
			}

			// Skip if no topics
			if startIdx >= len(topicNames) {
				return
			}

			// Get topics for this producer
			producerTopics := topicNames[startIdx:endIdx]

			log.Debug().
				Int("producer_id", producerID).
				Int("topics", len(producerTopics)).
				Int("start_idx", startIdx).
				Int("end_idx", endIdx).
				Msg("Producer started")

			// Create rate limiters for each topic
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

			// Track async publishes for proper acknowledgment handling
			type asyncPublishTracker struct {
				sendTime  time.Time
				topic     string
				future    jetstream.PubAckFuture
				processed bool // Flag to track if this publish has been processed already
			}

			// Use a slice for current batch messages
			currentBatchSize := 0
			pendingPublishes := make([]asyncPublishTracker, 0, b.config.BatchSize)

			// Main producer loop
			for {
				select {
				case <-b.done:
					// When shutting down, ensure all pending async publishes complete
					if !b.config.PubSync && len(pendingPublishes) > 0 {
						// Only process unprocessed publishes
						unprocessedCount := 0
						for i := range pendingPublishes {
							if !pendingPublishes[i].processed {
								unprocessedCount++
							}
						}

						if unprocessedCount > 0 {
							log.Debug().
								Int("producer_id", producerID).
								Int("remaining_publishes", unprocessedCount).
								Msg("Processing remaining publishes before exit")

							ctx, cancel := context.WithTimeout(context.Background(), b.config.RequestTimeout)
							defer cancel()

							select {
							case <-b.js.PublishAsyncComplete():
								// Process all remaining unprocessed acknowledgments
								for i := range pendingPublishes {
									if pendingPublishes[i].processed || pendingPublishes[i].future == nil {
										continue // Skip already processed ones
									}

									select {
									case <-pendingPublishes[i].future.Ok():
										// Record round-trip latency
										ackLatency := time.Since(pendingPublishes[i].sendTime)
										b.recordWriteLatency(ackLatency)

										// Count the message
										atomic.AddInt64(&b.messagesSent, 1)
										atomic.AddInt64(&b.bytesSent, int64(b.config.MessageSize))

										pendingPublishes[i].processed = true

										log.Debug().
											Dur("ack_latency", ackLatency).
											Str("topic", pendingPublishes[i].topic).
											Msg("Final publish acknowledged")
									case err := <-pendingPublishes[i].future.Err():
										log.Error().
											Err(err).
											Str("topic", pendingPublishes[i].topic).
											Msg("Final publish error")

										pendingPublishes[i].processed = true
									default:
										// Should not happen since PublishAsyncComplete signaled
										log.Warn().
											Str("topic", pendingPublishes[i].topic).
											Msg("Publish future not ready despite completion signal")
									}
								}
							case <-ctx.Done():
								log.Warn().
									Int("producer_id", producerID).
									Int("unprocessed", unprocessedCount).
									Msg("Timeout waiting for final async publishes")
							}
						}
					}
					return
				default:
					publishedThisIteration := false
					// Check each topic's ticker
					for i, topic := range producerTopics {
						select {
						case <-tickers[i].C:
							// It's time to send a message to this topic
							publishedThisIteration = true
							sendTime := time.Now()
							header := nats.Header{}
							header.Set("X-Sent-Time", sendTime.Format(time.RFC3339Nano))
							msg := &nats.Msg{
								Subject: topic,
								Header:  header,
								Data:    msgData,
							}

							if b.config.PubSync {
								// Synchronous publish with timeout
								ctx, cancel := context.WithTimeout(context.Background(), b.config.RequestTimeout)
								_, err := b.js.PublishMsg(ctx, msg)
								cancel()

								if err != nil {
									log.Error().Err(err).Str("topic", topic).Msg("Failed to publish message")
								} else {
									// For sync publishing, write latency includes the full round trip
									writeLatency := time.Since(sendTime)
									b.recordWriteLatency(writeLatency)

									// Count the message
									atomic.AddInt64(&b.messagesSent, 1)
									atomic.AddInt64(&b.bytesSent, int64(len(msgData)))
								}
							} else {
								// Asynchronous batch publish
								future, err := b.js.PublishMsgAsync(msg)
								if err != nil {
									log.Error().Err(err).Str("topic", topic).Msg("Failed to publish async message")
								} else {
									// Add to pending publishes for later acknowledgment handling
									pendingPublishes = append(pendingPublishes, asyncPublishTracker{
										sendTime:  sendTime,
										topic:     topic,
										future:    future,
										processed: false,
									})
									currentBatchSize++
								}
							}
						default:
							// Not time to send for this topic
						}
					}

					// Handle batch completion for async publishing
					if !b.config.PubSync && currentBatchSize >= b.config.BatchSize && len(pendingPublishes) > 0 {
						publishedThisIteration = true

						// First check if any futures are already ready
						for i := range pendingPublishes {
							if pendingPublishes[i].processed {
								continue // Skip already processed ones
							}

							// Try to get the acknowledgment without waiting
							select {
							case <-pendingPublishes[i].future.Ok():
								// Record full round-trip latency
								ackLatency := time.Since(pendingPublishes[i].sendTime)
								b.recordWriteLatency(ackLatency)

								// Count the message
								atomic.AddInt64(&b.messagesSent, 1)
								atomic.AddInt64(&b.bytesSent, int64(b.config.MessageSize))

								// Mark as processed
								pendingPublishes[i].processed = true

							case err := <-pendingPublishes[i].future.Err():
								log.Error().
									Err(err).
									Str("topic", pendingPublishes[i].topic).
									Msg("Async publish error")

								// Mark as processed even on error
								pendingPublishes[i].processed = true

							default:
								// Not ready yet, will be handled after PublishAsyncComplete
							}
						}

						// Count how many are still unprocessed
						unprocessedCount := 0
						for i := range pendingPublishes {
							if !pendingPublishes[i].processed {
								unprocessedCount++
							}
						}

						// If some are still unprocessed, wait for completion
						if unprocessedCount > 0 {
							ctx, cancel := context.WithTimeout(context.Background(), b.config.RequestTimeout)
							select {
							case <-b.js.PublishAsyncComplete():
								// Process remaining unprocessed acknowledgments
								for i := range pendingPublishes {
									if pendingPublishes[i].processed {
										continue // Skip already processed ones
									}

									select {
									case <-pendingPublishes[i].future.Ok():
										ackLatency := time.Since(pendingPublishes[i].sendTime)
										b.recordWriteLatency(ackLatency)

										// Count the message
										atomic.AddInt64(&b.messagesSent, 1)
										atomic.AddInt64(&b.bytesSent, int64(b.config.MessageSize))

										pendingPublishes[i].processed = true

									case err := <-pendingPublishes[i].future.Err():
										log.Error().
											Err(err).
											Str("topic", pendingPublishes[i].topic).
											Msg("Async publish error")

										pendingPublishes[i].processed = true

									default:
										// Should not happen after PublishAsyncComplete
										log.Warn().
											Str("topic", pendingPublishes[i].topic).
											Msg("Future not ready after PublishAsyncComplete")
									}
								}
							case <-ctx.Done():
								log.Warn().
									Int("unprocessed_count", unprocessedCount).
									Msg("Timeout waiting for publish acknowledgments")
							}
							cancel()
						}

						// Reset for next batch
						pendingPublishes = pendingPublishes[:0]
						currentBatchSize = 0
					}

					// If we didn't publish anything this iteration, yield the CPU briefly
					if !publishedThisIteration {
						runtime.Gosched()
					}
				}
			}
		}(p)
	}
}

// Note: We no longer need this function as the logic is now inlined where it's used
// to avoid the type mismatch error.

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

func formatDuration(d time.Duration) string {
	return fmt.Sprintf("%.3f ms", float64(d)/float64(time.Millisecond))
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

	// Calculate throughput in MB/s
	sendThroughputMBps := float64(bytesSent) / duration.Seconds() / (1024 * 1024)
	receiveThroughputMBps := float64(bytesReceived) / duration.Seconds() / (1024 * 1024)

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
		Float64("mb_sent_per_sec", sendThroughputMBps).
		Float64("mb_received_per_sec", receiveThroughputMBps).
		Int("stream_shards", b.config.StreamShards).
		Bool("use_republish", b.config.UseRepublish).
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
		// Sort for percentile calculations
		sort.Slice(writeLatenciesCopy, func(i, j int) bool {
			return writeLatenciesCopy[i] < writeLatenciesCopy[j]
		})

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

		// Calculate percentiles
		p50Write := b.getPercentile(writeLatenciesCopy, 50)
		p90Write := b.getPercentile(writeLatenciesCopy, 90)
		p95Write := b.getPercentile(writeLatenciesCopy, 95)
		p99Write := b.getPercentile(writeLatenciesCopy, 99)

		log.Info().
			Int("samples", len(writeLatenciesCopy)).
			Str("min", formatDuration(minWrite)).
			Str("max", formatDuration(maxWrite)).
			Str("avg", formatDuration(avgWrite)).
			Str("p50", formatDuration(p50Write)).
			Str("p90", formatDuration(p90Write)).
			Str("p95", formatDuration(p95Write)).
			Str("p99", formatDuration(p99Write)).
			Msg("Write Latency Statistics (ms)")
	} else {
		log.Info().Msg("No write latency samples collected")
	}

	// Calculate read latency stats
	if len(readLatenciesCopy) > 0 {
		// Sort for percentile calculations
		sort.Slice(readLatenciesCopy, func(i, j int) bool {
			return readLatenciesCopy[i] < readLatenciesCopy[j]
		})

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

		// Calculate percentiles
		p50Read := b.getPercentile(readLatenciesCopy, 50)
		p90Read := b.getPercentile(readLatenciesCopy, 90)
		p95Read := b.getPercentile(readLatenciesCopy, 95)
		p99Read := b.getPercentile(readLatenciesCopy, 99)

		log.Info().
			Int("samples", len(readLatenciesCopy)).
			Str("min", formatDuration(minRead)).
			Str("max", formatDuration(maxRead)).
			Str("avg", formatDuration(avgRead)).
			Str("p50", formatDuration(p50Read)).
			Str("p90", formatDuration(p90Read)).
			Str("p95", formatDuration(p95Read)).
			Str("p99", formatDuration(p99Read)).
			Msg("Read Latency Statistics (ms)")
	} else {
		log.Info().Msg("No read latency samples collected")
	}

	// Report subscription efficiency stats if using republish
	if b.config.UseRepublish {
		b.subMutex.Lock()
		subCount := len(b.subscriptions)
		b.subMutex.Unlock()

		log.Info().
			Bool("core_nats_subscribers", b.config.UseRepublish).
			Int("total_subjects", b.config.Concurrency).
			Int("subscriptions_created", subCount).
			Msg("Republish Stats")

		// Show subscription efficiency if any subscriptions were created
		if subCount > 0 {
			subjectPerSubRatio := float64(b.config.Concurrency) / float64(subCount)
			log.Info().
				Float64("subjects_per_subscription", subjectPerSubRatio).
				Msg("Subscription Efficiency")
		}

		// Show message delivery rate
		if msgsSent > 0 {
			deliveryRate := float64(msgsReceived) / float64(msgsSent) * 100
			log.Info().
				Float64("delivery_rate_percent", deliveryRate).
				Int64("messages_sent", msgsSent).
				Int64("messages_received", msgsReceived).
				Msg("Message Delivery Rate")
		}
	}
}
