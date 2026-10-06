package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"aeronelixir/bench/go/internal/benchkit"

	"github.com/andrewwormald/aergo"
)

type config struct {
	workers int
	size    int
	timeS   int
	warmupS int
}

type result struct {
	sent     int64
	received int64
	elapsed  time.Duration
	err      error
}

func main() {
	cfg := config{}
	flag.IntVar(&cfg.workers, "workers", 4, "worker goroutines, each with its own client")
	flag.IntVar(&cfg.size, "size", 256, "payload bytes")
	flag.IntVar(&cfg.timeS, "time", 5, "measured seconds")
	flag.IntVar(&cfg.warmupS, "warmup", 2, "warmup seconds")
	flag.Parse()

	fmt.Fprintf(os.Stderr, "[bench-go-mp] workers=%d size=%d time=%ds warmup=%ds dir=%s\n",
		cfg.workers, cfg.size, cfg.timeS, cfg.warmupS, benchkit.AeronDir())

	results := make([]result, cfg.workers)
	var group sync.WaitGroup
	for index := range cfg.workers {
		group.Add(1)
		go func() {
			defer group.Done()
			results[index] = runWorker(index, cfg)
		}()
	}
	group.Wait()

	var sent, received int64
	var slowest time.Duration
	for index, worker := range results {
		if worker.err != nil {
			benchkit.Fail("[bench-go-mp] worker %d: %v", index, worker.err)
		}
		sent += worker.sent
		received += worker.received
		slowest = max(slowest, worker.elapsed)
	}

	opsPerSec := float64(sent) / slowest.Seconds()
	fmt.Printf("{\"client\":\"go\",\"scenario\":\"throughput\",\"payload_size\":%d,\"workers\":%d,"+
		"\"samples\":%d,\"received\":%d,\"ops_per_sec\":%.3f,\"bytes_per_sec\":%.3f,\"elapsed_ms\":%.3f}\n",
		cfg.size, cfg.workers, sent, received, opsPerSec, opsPerSec*float64(cfg.size), float64(slowest)/1e6)
}

func runWorker(index int, cfg config) result {
	client, err := benchkit.Connect()
	if err != nil {
		return result{err: err}
	}
	defer client.Aeron.Close()

	channel := fmt.Sprintf("aeron:ipc?alias=go-mp-%d-%d|term-length=%s", index, time.Now().UnixNano(), benchkit.TermLength())
	streamID := int32((time.Now().UnixNano() + int64(index)) & 0x7FFFFFFF)

	pub, err := client.Aeron.AddPublication(channel, streamID)
	if err != nil {
		return result{err: fmt.Errorf("add publication: %w", err)}
	}
	sub, err := client.Aeron.AddSubscription(channel, streamID)
	if err != nil {
		return result{err: fmt.Errorf("add subscription: %w", err)}
	}
	if err := client.AwaitConnected(pub, 10*time.Second); err != nil {
		return result{err: err}
	}

	pool := benchkit.NewPool(cfg.size)

	var count, tickSum int64
	handler := func(buffer []byte, header *aergo.Header) {
		count++
		tickSum += benchkit.ReadTick(buffer)
	}
	poll := func() int {
		received := sub.Poll(handler, 1024)
		client.AfterPoll(received)
		return received
	}
	offer := func(message []byte, drain bool) error {
		for {
			outcome := pub.Offer(message)
			if outcome > 0 {
				return nil
			}
			if outcome == aergo.Closed || outcome == aergo.MaxPositionExceeded {
				return fmt.Errorf("publication closed: %d", outcome)
			}
			if drain {
				poll()
			} else {
				client.Service()
			}
		}
	}

	var sequence int64
	warmupDeadline := time.Now().Add(time.Duration(cfg.warmupS) * time.Second)
	for time.Now().Before(warmupDeadline) {
		message := pool.Message(sequence)
		sequence++
		if err := offer(message, false); err != nil {
			return result{err: err}
		}
		poll()
	}
	for poll() > 0 {
	}

	count, tickSum, sequence = 0, 0, 0
	var sent int64
	start := time.Now()
	runDeadline := start.Add(time.Duration(cfg.timeS) * time.Second)
	for time.Now().Before(runDeadline) {
		for range 1000 {
			message := pool.Message(sequence)
			sequence++
			if err := offer(message, true); err != nil {
				return result{err: err}
			}
			sent++
		}
		poll()
	}
	elapsed := time.Since(start)

	for count < sent && poll() > 0 {
	}

	fmt.Fprintf(os.Stderr, "[bench-go-mp] worker %d tick_sum=%d\n", index, tickSum)
	return result{sent: sent, received: count, elapsed: elapsed}
}
