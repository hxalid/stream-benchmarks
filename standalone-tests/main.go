package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Config holds the benchmark configuration
type Config struct {
	NatsURL          string        // NATS server URL
	StreamName       string        // JetStream stream name
	Concurrency      int           // Total number of subjects/streams
	ProducerCount    int           // Number of producer goroutines
	ConsumerCount    int           // Number of consumer goroutines
	Duration         time.Duration // Benchmark duration
	MessageSize      int           // Message size in bytes
	MessageRate      int           // Target message rate per second
	UseMemoryStorage bool          // Whether to use memory storage
	BatchSize        int           // Batch size for async publishing
	PubSync          bool          // Whether to use synchronous publishing
	RequestTimeout   time.Duration // Request timeout
	Replicas         int           // Number of stream replicas
}

// Benchmark implements the benchmark
type Benchmark struct {
	config         Config
	nc             *nats.Conn
	js             jetstream.JetStream
	subjects       []string             // All subjects
	subscriptions  []*nats.Subscription // Subscriptions to track and clean up
	subMutex       sync.Mutex           // Mutex for subscriptions
	done           chan struct{}        // Signal for shutdown
	messagesSent   int64                // Counter for sent messages
	messagesRecv   int64                // Counter for received messages
	bytesSent      int64                // Counter for bytes sent
	bytesRecv      int64                // Counter for bytes received
	writeLatencies []time.Duration      // Records write latencies
	readLatencies  []time.Duration      // Records read latencies
	latencyMutex   sync.Mutex           // Mutex for latency arrays
	producerWg     sync.WaitGroup       // WaitGroup for producers
	consumerWg     sync.WaitGroup       // WaitGroup for consumers
	startTime      time.Time            // Benchmark start time
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

// Setup prepares the benchmark
func (b *Benchmark) Setup() error {
	// Connect to NATS
	opts := []nats.Option{
		nats.Name("JetStream Benchmark"),
		nats.ReconnectWait(5 * time.Second),
		nats.MaxReconnects(10),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Printf("NATS disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("NATS reconnected to %s", nc.ConnectedUrl())
		}),
		nats.ErrorHandler(func(nc *nats.Conn, s *nats.Subscription, err error) {
			log.Printf("NATS error: %v", err)
		}),
		// Set a larger buffer size for high concurrency
		nats.ReconnectBufSize(128 * 1024 * 1024), // 128MB reconnect buffer
	}

	nc, err := nats.Connect(b.config.NatsURL, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}
	b.nc = nc

	// Create JetStream context with appropriate pending limits for high concurrency
	js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(1024))
	if err != nil {
		return fmt.Errorf("failed to create JetStream context: %w", err)
	}
	b.js = js

	// Create the stream with republish configuration
	err = b.createStream()
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	// Generate subject list
	b.generateSubjects()

	return nil
}

// createStream creates the JetStream stream with republish configuration
func (b *Benchmark) createStream() error {
	ctx := context.Background()

	storageType := jetstream.FileStorage
	if b.config.UseMemoryStorage {
		storageType = jetstream.MemoryStorage
	}

	// Create input stream with republish configuration
	log.Printf("Creating stream %s with republish to republished.>", b.config.StreamName)

	// Create stream with republish destination
	_, err := b.js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     b.config.StreamName,
		Subjects: []string{"events.>"},
		Storage:  storageType,
		Replicas: b.config.Replicas,
		RePublish: &jetstream.RePublish{
			Source:      "events.>",
			Destination: "republished.>",
		},
	})
	if err != nil && err.Error() != "stream name already in use" {
		log.Printf("Warning: Error creating stream: %v (continuing)", err)
	}

	return nil
}

// generateSubjects generates the list of subjects
func (b *Benchmark) generateSubjects() {
	// Generate all subject names
	totalSubjects := b.config.Concurrency
	b.subjects = make([]string, totalSubjects)

	for i := 0; i < totalSubjects; i++ {
		// Simple sequential naming for subjects
		b.subjects[i] = fmt.Sprintf("events.%d", i)
	}

	log.Printf("Generated %d subjects", len(b.subjects))
}

// Run executes the benchmark
func (b *Benchmark) Run() error {
	log.Printf("Starting benchmark with %d producers, %d subscribers, %d subjects",
		b.config.ProducerCount, b.config.ConsumerCount, len(b.subjects))

	// Record start time
	b.startTime = time.Now()

	// Start stats reporter
	go b.reportStats()

	// Start core NATS subscribers
	log.Printf("Starting core NATS subscribers...")
	b.startSubscribers()

	// Brief pause for subscribers to initialize
	time.Sleep(1 * time.Second)

	// Start producers
	log.Printf("Starting producers...")
	b.startProducers()

	// Set up signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Run for duration or until interrupted
	select {
	case <-time.After(b.config.Duration):
		log.Printf("Benchmark duration completed")
	case <-sigCh:
		log.Printf("Interrupted")
	}

	// Shut down benchmark
	close(b.done)

	// Wait for producers to finish
	log.Printf("Waiting for producers to finish...")
	b.producerWg.Wait()

	// Wait for subscribers to finish
	log.Printf("Waiting for subscribers to finish...")
	b.consumerWg.Wait()

	// Unsubscribe from all subscriptions
	b.cleanupSubscriptions()

	// Print final stats
	b.reportResults()

	return nil
}

// startProducers starts the producer goroutines
func (b *Benchmark) startProducers() {
	// Calculate subjects per producer
	subjectsPerProducer := (len(b.subjects) + b.config.ProducerCount - 1) / b.config.ProducerCount

	// Start producers
	for p := 0; p < b.config.ProducerCount; p++ {
		b.producerWg.Add(1)
		go func(producerID int) {
			defer b.producerWg.Done()

			// Calculate subject range for this producer
			startIdx := producerID * subjectsPerProducer
			endIdx := (producerID + 1) * subjectsPerProducer
			if endIdx > len(b.subjects) {
				endIdx = len(b.subjects)
			}

			// Skip if no subjects
			if startIdx >= len(b.subjects) {
				log.Printf("Producer %d has no subjects assigned", producerID)
				return
			}

			// Get subjects for this producer
			producerSubjects := b.subjects[startIdx:endIdx]
			log.Printf("Producer %d handling %d subjects", producerID, len(producerSubjects))

			// Prepare message data
			msgData := make([]byte, b.config.MessageSize)
			for i := range msgData {
				msgData[i] = byte(i % 256)
			}

			// Create message header template
			header := nats.Header{}
			header.Set("Producer-ID", fmt.Sprintf("%d", producerID))

			// For rate control and async publishing
			type subjectState struct {
				nextSendTime time.Time
				interval     time.Duration
				msgCount     int
				batchCounter int
				futures      []jetstream.PubAckFuture
			}

			// Initialize state for each subject
			states := make(map[string]*subjectState, len(producerSubjects))
			for _, subject := range producerSubjects {
				states[subject] = &subjectState{
					nextSendTime: time.Now(),
					interval:     time.Second / time.Duration(b.config.MessageRate),
					msgCount:     0,
					batchCounter: 0,
					futures:      make([]jetstream.PubAckFuture, 0, b.config.BatchSize),
				}
			}

			// Fast ticker for precise timing
			ticker := time.NewTicker(50 * time.Microsecond)
			defer ticker.Stop()

			// Main producer loop
			for {
				select {
				case <-b.done:
					// Process any remaining batches before exiting
					if !b.config.PubSync {
						for subj, state := range states {
							if len(state.futures) > 0 {
								b.processBatchAcks(subj, state.futures)
							}
						}
					}
					return
				case <-ticker.C:
					now := time.Now()

					// Check each subject
					for subject, state := range states {
						// If it's time to send a message for this subject
						if now.After(state.nextSendTime) {
							// Update next send time
							state.nextSendTime = state.nextSendTime.Add(state.interval)
							if state.nextSendTime.Before(now) {
								// If we fell behind, catch up
								state.nextSendTime = now.Add(state.interval)
							}

							// Prepare message with send time for latency tracking
							sendTime := time.Now()
							header.Set("X-Sent-Time", sendTime.Format(time.RFC3339Nano))
							header.Set("X-Subject", subject)

							// Create message
							msg := &nats.Msg{
								Subject: subject, // Publish to original subject (will be republished)
								Header:  header,
								Data:    msgData,
							}

							if b.config.PubSync {
								// Synchronous publish
								ctx, cancel := context.WithTimeout(context.Background(), b.config.RequestTimeout)
								_, err := b.js.PublishMsg(ctx, msg)
								cancel()

								if err != nil {
									if state.msgCount%1000 == 0 {
										log.Printf("Producer %d: Error publishing to %s: %v",
											producerID, subject, err)
									}
								} else {
									atomic.AddInt64(&b.messagesSent, 1)
									atomic.AddInt64(&b.bytesSent, int64(len(msgData)))
									state.msgCount++

									// Record write latency
									writeLatency := time.Since(sendTime)
									b.recordWriteLatency(writeLatency)

									if state.msgCount%1000 == 0 {
										log.Printf("Producer %d: Sent %d messages to %s",
											producerID, state.msgCount, subject)
									}
								}
							} else {
								// Asynchronous batch publish
								future, err := b.js.PublishMsgAsync(msg)
								if err != nil {
									if state.msgCount%1000 == 0 {
										log.Printf("Producer %d: Error async publishing to %s: %v",
											producerID, subject, err)
									}
								} else {
									state.futures = append(state.futures, future)
									state.batchCounter++

									// Process batch when full
									if state.batchCounter >= b.config.BatchSize {
										b.processBatchAcks(subject, state.futures)

										// Reset batch
										state.batchCounter = 0
										state.futures = make([]jetstream.PubAckFuture, 0, b.config.BatchSize)
										state.msgCount += b.config.BatchSize

										if state.msgCount%1000 == 0 {
											log.Printf("Producer %d: Processed %d batch messages for %s",
												producerID, state.msgCount, subject)
										}
									}
								}
							}
						}
					}
				}
			}
		}(p)
	}
}

// processBatchAcks processes batch acknowledgements
func (b *Benchmark) processBatchAcks(subject string, futures []jetstream.PubAckFuture) {
	// Save current time for latency calculation
	sendTime := time.Now()

	// Process each future in the batch
	for _, future := range futures {
		select {
		case ack := <-future.Ok():
			atomic.AddInt64(&b.messagesSent, 1)
			atomic.AddInt64(&b.bytesSent, int64(b.config.MessageSize))

			// Record latency
			b.recordWriteLatency(time.Since(sendTime))

			// Use sequence for debugging if needed
			_ = ack.Sequence
		case err := <-future.Err():
			log.Printf("Publish error for %s: %v", subject, err)
		case <-time.After(b.config.RequestTimeout):
			log.Printf("Timeout waiting for ack for %s", subject)
		}
	}
}

// startSubscribers starts core NATS subscribers
func (b *Benchmark) startSubscribers() {
	// Calculate subjects per subscriber
	totalSubjects := b.config.Concurrency
	subjectsPerSubscriber := (totalSubjects + b.config.ConsumerCount - 1) / b.config.ConsumerCount

	// Start subscribers
	for c := 0; c < b.config.ConsumerCount; c++ {
		b.consumerWg.Add(1)
		go func(subscriberID int) {
			defer b.consumerWg.Done()

			// Calculate subject range for this subscriber
			startIdx := subscriberID * subjectsPerSubscriber
			endIdx := (subscriberID + 1) * subjectsPerSubscriber
			if endIdx > totalSubjects {
				endIdx = totalSubjects
			}

			// Skip if no subjects assigned
			if startIdx >= totalSubjects {
				log.Printf("Subscriber %d has no subjects assigned", subscriberID)
				return
			}

			log.Printf("Subscriber %d handling republished subjects %d through %d",
				subscriberID, startIdx, endIdx-1)

			// Subscribe to all subjects in range
			for subjectIdx := startIdx; subjectIdx < endIdx; subjectIdx++ {
				// Create pattern subscription for the republished subject
				// Original: events.N -> Republished: republished.N
				subjectPattern := fmt.Sprintf("republished.%d", subjectIdx)

				// Create core NATS subscription
				sub, err := b.nc.Subscribe(subjectPattern, func(msg *nats.Msg) {
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
					atomic.AddInt64(&b.messagesRecv, 1)
					atomic.AddInt64(&b.bytesRecv, int64(len(msg.Data)))
				})

				if err != nil {
					log.Printf("Subscriber %d: Error subscribing to %s: %v",
						subscriberID, subjectPattern, err)
					continue
				}

				// Store subscription for cleanup
				b.subMutex.Lock()
				b.subscriptions = append(b.subscriptions, sub)
				b.subMutex.Unlock()
			}

			// Log subscription count
			log.Printf("Subscriber %d: Created %d subscriptions",
				subscriberID, endIdx-startIdx)

			// Wait until done
			<-b.done
		}(c)
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

	log.Printf("Cleaned up %d subscriptions", len(b.subscriptions))
	b.subscriptions = nil
}

// recordReadLatency safely records a read latency value
func (b *Benchmark) recordReadLatency(latency time.Duration) {
	b.latencyMutex.Lock()
	b.readLatencies = append(b.readLatencies, latency)
	b.latencyMutex.Unlock()
}

// recordWriteLatency safely records a write latency value
func (b *Benchmark) recordWriteLatency(latency time.Duration) {
	b.latencyMutex.Lock()
	b.writeLatencies = append(b.writeLatencies, latency)
	b.latencyMutex.Unlock()
}

// getPercentile safely gets a percentile value from a sorted duration slice
func getPercentile(sortedDurations []time.Duration, percentile int) time.Duration {
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

// reportStats reports benchmark statistics periodically
func (b *Benchmark) reportStats() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	lastSent := int64(0)
	lastRecv := int64(0)
	lastBytesSent := int64(0)
	lastBytesRecv := int64(0)
	lastTime := time.Now()
	benchStartTime := b.startTime

	// Main stats reporting loop
	for {
		select {
		case <-b.done:
			return
		case <-ticker.C:
			now := time.Now()
			elapsed := now.Sub(lastTime)
			totalElapsed := now.Sub(benchStartTime)
			elapsedSeconds := elapsed.Seconds()
			totalElapsedSeconds := totalElapsed.Seconds()

			// Avoid division by zero
			if elapsedSeconds <= 0 {
				continue
			}

			// Get current counts
			currentSent := atomic.LoadInt64(&b.messagesSent)
			currentRecv := atomic.LoadInt64(&b.messagesRecv)
			currentBytesSent := atomic.LoadInt64(&b.bytesSent)
			currentBytesRecv := atomic.LoadInt64(&b.bytesRecv)

			// Calculate rates (for this interval)
			sendRate := float64(currentSent-lastSent) / elapsedSeconds
			recvRate := float64(currentRecv-lastRecv) / elapsedSeconds
			bytesSentRate := float64(currentBytesSent-lastBytesSent) / elapsedSeconds
			bytesRecvRate := float64(currentBytesRecv-lastBytesRecv) / elapsedSeconds

			// Calculate average rates (over the entire benchmark)
			avgSendRate := float64(currentSent) / totalElapsedSeconds
			avgRecvRate := float64(currentRecv) / totalElapsedSeconds

			// Per-subject rates - ensure no division by zero
			totalSubjects := float64(b.config.Concurrency)
			var perSubjectSendRate, perSubjectRecvRate float64
			var avgPerSubjectSendRate, avgPerSubjectRecvRate float64
			if totalSubjects > 0 {
				perSubjectSendRate = sendRate / totalSubjects
				perSubjectRecvRate = recvRate / totalSubjects
				avgPerSubjectSendRate = avgSendRate / totalSubjects
				avgPerSubjectRecvRate = avgRecvRate / totalSubjects
			}

			// Convert bytes to MB
			mbSent := bytesSentRate / (1024 * 1024)
			mbRecv := bytesRecvRate / (1024 * 1024)

			// Calculate and report current latency stats
			var p50WriteLatency, p95WriteLatency, p99WriteLatency time.Duration
			var p50ReadLatency, p95ReadLatency, p99ReadLatency time.Duration

			// Safely access latency arrays
			b.latencyMutex.Lock()
			writeSampleCount := len(b.writeLatencies)
			readSampleCount := len(b.readLatencies)

			// Process write latencies if we have samples
			if writeSampleCount > 0 {
				// Create a copy to avoid modifying the original slice while sorting
				wLatencies := make([]time.Duration, writeSampleCount)
				copy(wLatencies, b.writeLatencies)

				// Sort for percentile calculation
				sort.Slice(wLatencies, func(i, j int) bool {
					return wLatencies[i] < wLatencies[j]
				})

				// Calculate percentiles with safe index access
				p50WriteLatency = getPercentile(wLatencies, 50)
				p95WriteLatency = getPercentile(wLatencies, 95)
				p99WriteLatency = getPercentile(wLatencies, 99)
			}

			// Process read latencies if we have samples
			if readSampleCount > 0 {
				// Create a copy to avoid modifying the original slice while sorting
				rLatencies := make([]time.Duration, readSampleCount)
				copy(rLatencies, b.readLatencies)

				// Sort for percentile calculation
				sort.Slice(rLatencies, func(i, j int) bool {
					return rLatencies[i] < rLatencies[j]
				})

				// Calculate percentiles with safe index access
				p50ReadLatency = getPercentile(rLatencies, 50)
				p95ReadLatency = getPercentile(rLatencies, 95)
				p99ReadLatency = getPercentile(rLatencies, 99)
			}

			// Reset latency arrays periodically to prevent excessive memory usage
			if writeSampleCount > 100000 {
				b.writeLatencies = b.writeLatencies[writeSampleCount-50000:]
			}
			if readSampleCount > 100000 {
				b.readLatencies = b.readLatencies[readSampleCount-50000:]
			}
			b.latencyMutex.Unlock()

			// Log stats with all the calculations
			log.Printf("STATS [%.1fs] SENT: %d (%.1f/sec total, %.3f/subject/sec curr, %.3f/subject/sec avg) RECV: %d (%.1f/sec total, %.3f/subject/sec curr, %.3f/subject/sec avg) MB: %.1f sent, %.1f recv, Latency(ms) Write p50/p95/p99: %.3f/%.3f/%.3f, Read p50/p95/p99: %.3f/%.3f/%.3f",
				time.Since(b.startTime).Seconds(),
				currentSent, sendRate, perSubjectSendRate, avgPerSubjectSendRate,
				currentRecv, recvRate, perSubjectRecvRate, avgPerSubjectRecvRate,
				mbSent, mbRecv,
				float64(p50WriteLatency)/float64(time.Millisecond),
				float64(p95WriteLatency)/float64(time.Millisecond),
				float64(p99WriteLatency)/float64(time.Millisecond),
				float64(p50ReadLatency)/float64(time.Millisecond),
				float64(p95ReadLatency)/float64(time.Millisecond),
				float64(p99ReadLatency)/float64(time.Millisecond))

			// Update saved values for next iteration
			lastSent = currentSent
			lastRecv = currentRecv
			lastBytesSent = currentBytesSent
			lastBytesRecv = currentBytesRecv
			lastTime = now
		}
	}
}

// reportResults calculates and reports the final benchmark results
func (b *Benchmark) reportResults() {
	duration := time.Since(b.startTime)
	durationSeconds := duration.Seconds()
	msgsSent := atomic.LoadInt64(&b.messagesSent)
	msgsReceived := atomic.LoadInt64(&b.messagesRecv)
	bytesSent := atomic.LoadInt64(&b.bytesSent)
	bytesReceived := atomic.LoadInt64(&b.bytesRecv)

	// Calculate message rates
	sendRate := float64(msgsSent) / durationSeconds
	receiveRate := float64(msgsReceived) / durationSeconds

	// Per-subject rates
	perSubjectSendRate := sendRate / float64(b.config.Concurrency)
	perSubjectRecvRate := receiveRate / float64(b.config.Concurrency)

	// Calculate throughput in MB/s
	sendThroughputMBps := float64(bytesSent) / durationSeconds / (1024 * 1024)
	receiveThroughputMBps := float64(bytesReceived) / durationSeconds / (1024 * 1024)

	// Report overall performance
	log.Printf("=== BENCHMARK RESULTS ===")
	log.Printf("Duration: %.2f seconds", durationSeconds)
	log.Printf("Configuration:")
	log.Printf("  Stream: %s", b.config.StreamName)
	log.Printf("  Concurrency (Total Subjects): %d", b.config.Concurrency)
	log.Printf("  Producers: %d", b.config.ProducerCount)
	log.Printf("  Subscribers: %d", b.config.ConsumerCount)
	log.Printf("  Message Size: %d bytes", b.config.MessageSize)
	log.Printf("  Target Rate: %d msgs/sec per subject", b.config.MessageRate)
	log.Printf("  Batch Size: %d", b.config.BatchSize)
	log.Printf("  Sync Publishing: %v", b.config.PubSync)
	log.Printf("  Memory Storage: %v", b.config.UseMemoryStorage)
	log.Printf("  Replicas: %d", b.config.Replicas)

	log.Printf("Performance:")
	log.Printf("  Messages Sent: %d", msgsSent)
	log.Printf("    Total Rate: %.1f msgs/sec", sendRate)
	log.Printf("    Per Subject Rate: %.3f msgs/sec/subject", perSubjectSendRate)
	log.Printf("    Per Producer Rate: %.1f msgs/sec/producer", sendRate/float64(b.config.ProducerCount))
	log.Printf("  Messages Received: %d", msgsReceived)
	log.Printf("    Total Rate: %.1f msgs/sec", receiveRate)
	log.Printf("    Per Subject Rate: %.3f msgs/sec/subject", perSubjectRecvRate)
	log.Printf("    Per Subscriber Rate: %.1f msgs/sec/subscriber", receiveRate/float64(b.config.ConsumerCount))
	log.Printf("  Throughput:")
	log.Printf("    Sent: %.2f MB/s (%.2f Mbps)", sendThroughputMBps, sendThroughputMBps*8)
	log.Printf("    Received: %.2f MB/s (%.2f Mbps)", receiveThroughputMBps, receiveThroughputMBps*8)

	// Calculate latency statistics
	b.latencyMutex.Lock()
	writeLatenciesCopy := make([]time.Duration, len(b.writeLatencies))
	copy(writeLatenciesCopy, b.writeLatencies)

	readLatenciesCopy := make([]time.Duration, len(b.readLatencies))
	copy(readLatenciesCopy, b.readLatencies)
	b.latencyMutex.Unlock()

	// Calculate write latency stats
	if len(writeLatenciesCopy) > 0 {
		// Sort latencies
		sort.Slice(writeLatenciesCopy, func(i, j int) bool {
			return writeLatenciesCopy[i] < writeLatenciesCopy[j]
		})

		p50 := getPercentile(writeLatenciesCopy, 50)
		p90 := getPercentile(writeLatenciesCopy, 90)
		p95 := getPercentile(writeLatenciesCopy, 95)
		p99 := getPercentile(writeLatenciesCopy, 99)

		log.Printf("Write Latency (ms):")
		log.Printf("  Samples: %d", len(writeLatenciesCopy))
		log.Printf("  p50: %.3f", float64(p50)/float64(time.Millisecond))
		log.Printf("  p90: %.3f", float64(p90)/float64(time.Millisecond))
		log.Printf("  p95: %.3f", float64(p95)/float64(time.Millisecond))
		log.Printf("  p99: %.3f", float64(p99)/float64(time.Millisecond))

		// Calculate average
		var sum time.Duration
		for _, d := range writeLatenciesCopy {
			sum += d
		}
		avgWrite := sum / time.Duration(len(writeLatenciesCopy))
		log.Printf("  Average: %.3f ms", float64(avgWrite)/float64(time.Millisecond))
	} else {
		log.Printf("No write latency samples collected")
	}

	// Calculate read latency stats
	if len(readLatenciesCopy) > 0 {
		// Sort latencies
		sort.Slice(readLatenciesCopy, func(i, j int) bool {
			return readLatenciesCopy[i] < readLatenciesCopy[j]
		})

		p50 := getPercentile(readLatenciesCopy, 50)
		p90 := getPercentile(readLatenciesCopy, 90)
		p95 := getPercentile(readLatenciesCopy, 95)
		p99 := getPercentile(readLatenciesCopy, 99)

		log.Printf("Read Latency (ms):")
		log.Printf("  Samples: %d", len(readLatenciesCopy))
		log.Printf("  p50: %.3f", float64(p50)/float64(time.Millisecond))
		log.Printf("  p90: %.3f", float64(p90)/float64(time.Millisecond))
		log.Printf("  p95: %.3f", float64(p95)/float64(time.Millisecond))
		log.Printf("  p99: %.3f", float64(p99)/float64(time.Millisecond))

		// Calculate average
		var sum time.Duration
		for _, d := range readLatenciesCopy {
			sum += d
		}
		avgRead := sum / time.Duration(len(readLatenciesCopy))
		log.Printf("  Average: %.3f ms", float64(avgRead)/float64(time.Millisecond))
	} else {
		log.Printf("No read latency samples collected")
	}

	// Report republish efficiency stats
	b.subMutex.Lock()
	subCount := len(b.subscriptions)
	b.subMutex.Unlock()

	log.Printf("Republish Stats:")
	log.Printf("  Using Core NATS Subscribers: Yes")
	log.Printf("  Total Subjects: %d", b.config.Concurrency)
	log.Printf("  Subscriptions Created: %d", subCount)

	// Show subscription efficiency if any subscriptions were created
	if subCount > 0 {
		subjectPerSubRatio := float64(b.config.Concurrency) / float64(subCount)
		log.Printf("  Subscription Efficiency: %.1f subjects per subscription", subjectPerSubRatio)
	}

	// Show message delivery rate
	if msgsSent > 0 {
		deliveryRate := float64(msgsReceived) / float64(msgsSent) * 100
		log.Printf("  Message Delivery Rate: %.1f%% (%d sent, %d received)",
			deliveryRate, msgsSent, msgsReceived)
	}
}

// main function
func main() {
	// Parse command line flags
	natsURL := flag.String("nats", "nats://localhost:4222", "NATS server URL")
	streamName := flag.String("stream", "BENCHMARK_STREAM", "Stream name")
	concurrency := flag.Int("concurrency", 13000, "Number of subjects/concurrency")
	producerCount := flag.Int("producers", 100, "Number of producer goroutines")
	consumerCount := flag.Int("subscribers", 100, "Number of subscriber goroutines")
	duration := flag.Duration("duration", 1*time.Minute, "Benchmark duration")
	messageSize := flag.Int("size", 2048, "Message size in bytes")
	messageRate := flag.Int("rate", 10, "Target messages per second per subject")
	useMemoryStorage := flag.Bool("memory", true, "Use memory storage")
	batchSize := flag.Int("batch", 100, "Batch size for async publishing")
	pubSync := flag.Bool("sync", false, "Use synchronous publishing")
	requestTimeout := flag.Duration("timeout", 3*time.Second, "Request timeout")
	replicas := flag.Int("replicas", 1, "Number of stream replicas")

	flag.Parse()

	// Create benchmark configuration
	config := Config{
		NatsURL:          *natsURL,
		StreamName:       *streamName,
		Concurrency:      *concurrency,
		ProducerCount:    *producerCount,
		ConsumerCount:    *consumerCount,
		Duration:         *duration,
		MessageSize:      *messageSize,
		MessageRate:      *messageRate,
		UseMemoryStorage: *useMemoryStorage,
		BatchSize:        *batchSize,
		PubSync:          *pubSync,
		RequestTimeout:   *requestTimeout,
		Replicas:         *replicas,
	}

	// Create and run benchmark
	benchmark := NewBenchmark(config)

	log.Printf("Setting up benchmark with %d subjects using core NATS subscribers and JetStream republish",
		config.Concurrency)

	if err := benchmark.Setup(); err != nil {
		log.Fatalf("Setup error: %v", err)
	}

	log.Printf("Starting benchmark run for %s", config.Duration)
	if err := benchmark.Run(); err != nil {
		log.Fatalf("Run error: %v", err)
	}

	log.Printf("Benchmark completed successfully")
}
