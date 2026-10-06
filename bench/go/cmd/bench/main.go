package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"aeronelixir/bench/go/internal/benchkit"

	"github.com/andrewwormald/aergo"
)

type config struct {
	mode    string
	size    int
	timeS   int
	warmupS int
}

type counter struct {
	count  int64
	bidSum int64
}

func main() {
	cfg := parseArgs()
	fmt.Fprintf(os.Stderr, "[bench-go] mode=%s size=%d time=%ds warmup=%ds dir=%s\n",
		cfg.mode, cfg.size, cfg.timeS, cfg.warmupS, benchkit.AeronDir())

	client, err := benchkit.Connect()
	if err != nil {
		benchkit.Fail("[bench-go] %v", err)
	}
	defer client.Aeron.Close()

	channel := fmt.Sprintf("aeron:ipc?alias=go-bench-%d|term-length=%s", time.Now().UnixNano(), benchkit.TermLength())
	streamID := int32(time.Now().UnixNano() & 0x7FFFFFFF)

	pub, err := client.Aeron.AddPublication(channel, streamID)
	if err != nil {
		benchkit.Fail("[bench-go] add publication: %v", err)
	}
	sub, err := client.Aeron.AddSubscription(channel, streamID)
	if err != nil {
		benchkit.Fail("[bench-go] add subscription: %v", err)
	}
	if err := client.AwaitConnected(pub, 10*time.Second); err != nil {
		benchkit.Fail("[bench-go] %v", err)
	}

	pool := benchkit.NewPool(cfg.size)
	switch cfg.mode {
	case "latency":
		runLatency(client, pub, sub, pool, cfg)
	case "throughput":
		runThroughput(client, pub, sub, pool, cfg)
	default:
		benchkit.Fail("[bench-go] unknown mode %q", cfg.mode)
	}
}

func parseArgs() config {
	cfg := config{}
	flag.StringVar(&cfg.mode, "mode", "latency", "latency or throughput")
	flag.IntVar(&cfg.size, "size", 32, "payload bytes")
	flag.IntVar(&cfg.timeS, "time", 10, "measured seconds")
	flag.IntVar(&cfg.warmupS, "warmup", 3, "warmup seconds")
	flag.Parse()
	return cfg
}

func offerUntilAccepted(client *benchkit.Client, pub *aergo.Publication, payload []byte) {
	for {
		result := pub.Offer(payload)
		if result > 0 {
			return
		}
		if result == aergo.Closed || result == aergo.MaxPositionExceeded {
			benchkit.Fail("[bench-go] publication closed: %d", result)
		}
		client.Service()
	}
}

func drainOne(client *benchkit.Client, sub *aergo.Subscription) {
	received := 0
	handler := func(buffer []byte, header *aergo.Header) { received++ }
	for received == 0 {
		client.AfterPoll(sub.Poll(handler, 1))
	}
}

func runLatency(client *benchkit.Client, pub *aergo.Publication, sub *aergo.Subscription, pool *benchkit.Pool, cfg config) {
	var sequence int64
	warmupDeadline := time.Now().Add(time.Duration(cfg.warmupS) * time.Second)
	for time.Now().Before(warmupDeadline) {
		offerUntilAccepted(client, pub, pool.Message(sequence))
		sequence++
		drainOne(client, sub)
	}

	samples := make([]int64, 0, cfg.timeS*10_000_000)
	start := time.Now()
	runDeadline := start.Add(time.Duration(cfg.timeS) * time.Second)
	for len(samples) < cap(samples) && time.Now().Before(runDeadline) {
		t0 := time.Now()
		offerUntilAccepted(client, pub, pool.Message(sequence))
		sequence++
		drainOne(client, sub)
		samples = append(samples, int64(time.Since(t0)))
	}
	elapsed := time.Since(start)

	mean := benchkit.MeanMicros(samples)
	benchkit.SortSamples(samples)
	fmt.Printf("{\"client\":\"go\",\"scenario\":\"latency\",\"payload_size\":%d,\"samples\":%d,"+
		"\"ops_per_sec\":%.3f,\"elapsed_ms\":%.3f,\"mean_us\":%.4f,\"median_us\":%.4f,"+
		"\"p99_us\":%.4f,\"p999_us\":%.4f,\"min_us\":%.4f,\"max_us\":%.4f}\n",
		cfg.size, len(samples), float64(len(samples))/elapsed.Seconds(), float64(elapsed)/1e6,
		mean, benchkit.PercentileMicros(samples, 50), benchkit.PercentileMicros(samples, 99),
		benchkit.PercentileMicros(samples, 99.9), benchkit.PercentileMicros(samples, 0),
		float64(samples[len(samples)-1])/1000.0)
}

func runThroughput(client *benchkit.Client, pub *aergo.Publication, sub *aergo.Subscription, pool *benchkit.Pool, cfg config) {
	state := &counter{}
	handler := func(buffer []byte, header *aergo.Header) {
		state.count++
		state.bidSum += benchkit.ReadTick(buffer)
	}
	poll := func() int {
		received := sub.Poll(handler, 1024)
		client.AfterPoll(received)
		return received
	}
	offerWithDrain := func(message []byte) {
		for {
			result := pub.Offer(message)
			if result > 0 {
				return
			}
			if result == aergo.Closed || result == aergo.MaxPositionExceeded {
				benchkit.Fail("[bench-go] publication closed: %d", result)
			}
			poll()
		}
	}

	var sequence int64
	warmupDeadline := time.Now().Add(time.Duration(cfg.warmupS) * time.Second)
	for time.Now().Before(warmupDeadline) {
		offerUntilAccepted(client, pub, pool.Message(sequence))
		sequence++
		poll()
	}
	for poll() > 0 {
	}

	state.count, state.bidSum, sequence = 0, 0, 0
	var sent int64
	start := time.Now()
	runDeadline := start.Add(time.Duration(cfg.timeS) * time.Second)
	for time.Now().Before(runDeadline) {
		for i := 0; i < 1000; i++ {
			offerWithDrain(pool.Message(sequence))
			sequence++
			sent++
		}
		poll()
	}
	elapsed := time.Since(start)

	for state.count < sent && poll() > 0 {
	}

	fmt.Fprintf(os.Stderr, "[bench-go] throughput bid_sum=%d\n", state.bidSum)
	opsPerSec := float64(sent) / elapsed.Seconds()
	fmt.Printf("{\"client\":\"go\",\"scenario\":\"throughput\",\"payload_size\":%d,\"samples\":%d,"+
		"\"ops_per_sec\":%.3f,\"bytes_per_sec\":%.3f,\"received\":%d,\"elapsed_ms\":%.3f}\n",
		cfg.size, sent, opsPerSec, opsPerSec*float64(cfg.size), state.count, float64(elapsed)/1e6)
}
