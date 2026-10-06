package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"aeronelixir/bench/go/internal/benchkit"

	"github.com/andrewwormald/aergo"
)

const startSpinNs = 2_000_000

type config struct {
	mode       string
	pingStream int32
	pongStream int32
	length     int
	warmup     int64
	messages   int64
	startAtNs  int64
}

func main() {
	cfg := parseArgs()
	fmt.Fprintf(os.Stderr, "[bench-go-rpc] mode=%s ping=%d pong=%d length=%d dir=%s\n",
		cfg.mode, cfg.pingStream, cfg.pongStream, cfg.length, benchkit.AeronDir())

	client, err := benchkit.Connect()
	if err != nil {
		benchkit.Fail("[bench-go-rpc] %v", err)
	}
	defer client.Aeron.Close()

	switch cfg.mode {
	case "ping":
		runPing(client, cfg)
	case "pong":
		runPong(client, cfg)
	default:
		benchkit.Fail("[bench-go-rpc] unknown mode %q", cfg.mode)
	}
}

func parseArgs() config {
	base := envInt("RPC_STREAM_BASE", 7100)
	pairIndex := envInt("RPC_PAIR_INDEX", 0)
	cfg := config{
		pingStream: int32(base + pairIndex*2),
		pongStream: int32(base + pairIndex*2 + 1),
		length:     envInt("RPC_MESSAGE_LENGTH", 32),
		warmup:     int64(envInt("RPC_WARMUP_MESSAGES", 100000)),
		messages:   int64(envInt("RPC_MESSAGES", 1000000)),
		startAtNs:  int64(envInt("RPC_START_AT_NS", 0)),
	}
	flag.StringVar(&cfg.mode, "mode", "ping", "ping or pong")
	flag.Parse()
	return cfg
}

func envInt(name string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil {
		return fallback
	}
	return value
}

func addStreams(client *benchkit.Client, pubStream, subStream int32) (*aergo.Publication, *aergo.Subscription) {
	sub, err := client.Aeron.AddSubscription("aeron:ipc", subStream)
	if err != nil {
		benchkit.Fail("[bench-go-rpc] add subscription: %v", err)
	}
	pub, err := client.Aeron.AddPublication("aeron:ipc", pubStream)
	if err != nil {
		benchkit.Fail("[bench-go-rpc] add publication: %v", err)
	}
	return pub, sub
}

func offer(client *benchkit.Client, pub *aergo.Publication, payload []byte) {
	for {
		result := pub.Offer(payload)
		if result > 0 {
			return
		}
		if result == aergo.Closed || result == aergo.MaxPositionExceeded {
			benchkit.Fail("[bench-go-rpc] publication closed: %d", result)
		}
		client.Service()
	}
}

func runPing(client *benchkit.Client, cfg config) {
	pub, sub := addStreams(client, cfg.pingStream, cfg.pongStream)
	if err := client.AwaitConnected(pub, 30*time.Second); err != nil {
		benchkit.Fail("[bench-go-rpc] %v", err)
	}

	pool := benchkit.NewPool(cfg.length)

	var received, tickSum int64
	handler := func(buffer []byte, header *aergo.Header) {
		received++
		tickSum += benchkit.ReadTick(buffer)
	}
	var sequence int64
	roundTrip := func() {
		offer(client, pub, pool.Message(sequence))
		sequence++
		received = 0
		for received == 0 {
			client.AfterPoll(sub.Poll(handler, 1))
		}
	}

	for range cfg.warmup {
		roundTrip()
	}

	samples := make([]int64, cfg.messages)
	awaitStart(cfg.startAtNs)
	startedAt := time.Now().UnixNano()
	start := time.Now()
	for i := range samples {
		t0 := time.Now()
		roundTrip()
		samples[i] = int64(time.Since(t0))
	}
	elapsed := time.Since(start)
	finishedAt := time.Now().UnixNano()

	mean := benchkit.MeanMicros(samples)
	benchkit.SortSamples(samples)
	fmt.Fprintf(os.Stderr, "[bench-go-rpc] tick_sum=%d\n", tickSum)
	fmt.Printf("{\"client\":\"go\",\"scenario\":\"rpc\",\"pairs\":1,\"message_length\":%d,\"samples\":%d,"+
		"\"elapsed_ms\":%.3f,\"mean_us\":%.4f,\"p50_us\":%.4f,\"p90_us\":%.4f,\"p99_us\":%.4f,"+
		"\"p999_us\":%.4f,\"max_us\":%.4f,\"round_trips_per_sec\":%.3f,\"started_at_ns\":%d,\"finished_at_ns\":%d}\n",
		cfg.length, cfg.messages, float64(elapsed)/1e6, mean,
		benchkit.PercentileMicros(samples, 50), benchkit.PercentileMicros(samples, 90),
		benchkit.PercentileMicros(samples, 99), benchkit.PercentileMicros(samples, 99.9),
		float64(samples[len(samples)-1])/1000.0, float64(cfg.messages)/elapsed.Seconds(),
		startedAt, finishedAt)
}

func awaitStart(startAtNs int64) {
	if sleepNs := startAtNs - time.Now().UnixNano() - startSpinNs; sleepNs > 0 {
		time.Sleep(time.Duration(sleepNs))
	}
	for time.Now().UnixNano() < startAtNs {
	}
}

func runPong(client *benchkit.Client, cfg config) {
	var running atomic.Bool
	running.Store(true)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-signals
		running.Store(false)
	}()

	pub, sub := addStreams(client, cfg.pongStream, cfg.pingStream)
	fmt.Println("READY")
	if err := client.AwaitConnected(pub, 30*time.Second); err != nil {
		benchkit.Fail("[bench-go-rpc] %v", err)
	}

	var received, tickSum int64
	handler := func(buffer []byte, header *aergo.Header) {
		received++
		tickSum += benchkit.ReadTick(buffer)
		offer(client, pub, buffer)
	}
	for running.Load() {
		client.AfterPoll(sub.Poll(handler, 1))
	}
	fmt.Fprintf(os.Stderr, "[bench-go-rpc] pong tick_sum=%d received=%d\n", tickSum, received)
}
