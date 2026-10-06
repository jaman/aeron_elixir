package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"time"

	"aeronelixir/bench/go/internal/benchkit"

	"github.com/andrewwormald/aergo"
)

const (
	buckets  = 100000
	bucketNs = 100
	idleExit = 750 * time.Millisecond
)

type consumerState struct {
	consumed      int64
	firstSequence int64
	lastSequence  int64
	gaps          int64
	priceSum      int64
	negatives     int64
	maxNs         int64
	counts        []int64
}

func main() {
	channel := benchkit.EnvOr("HARNESS_CHANNEL", "aeron:ipc?alias=prices|term-length=67108864")
	stream, _ := strconv.Atoi(benchkit.EnvOr("HARNESS_STREAM", "9001"))
	durationS, _ := strconv.Atoi(benchkit.EnvOr("HARNESS_DURATION_S", "30"))
	label := benchkit.EnvOr("HARNESS_LABEL", "go")

	client, err := benchkit.Connect()
	if err != nil {
		benchkit.Fail("[consumer-%s] %v", label, err)
	}
	defer client.Aeron.Close()

	sub, err := client.Aeron.AddSubscription(channel, int32(stream))
	if err != nil {
		benchkit.Fail("[consumer-%s] add subscription: %v", label, err)
	}
	fmt.Println("READY")
	fmt.Fprintf(os.Stderr, "[consumer-%s] subscribed\n", label)

	state := &consumerState{firstSequence: -1, lastSequence: -1, counts: make([]int64, buckets)}
	assembler := aergo.NewFragmentAssembler(func(buffer []byte, header *aergo.Header) { state.onTick(buffer) })

	deadline := time.Now().Add(time.Duration(durationS+10) * time.Second)
	lastMessageAt := time.Now()
	for {
		now := time.Now()
		if now.After(deadline) || (state.consumed > 0 && now.Sub(lastMessageAt) > idleExit) {
			break
		}
		received := sub.Poll(assembler.OnFragment, 1024)
		if received > 0 {
			lastMessageAt = now
		}
		client.AfterPoll(received)
	}

	state.report(label)
}

func (s *consumerState) onTick(buffer []byte) {
	if len(buffer) < 36 {
		return
	}
	instrumentID := int64(binary.LittleEndian.Uint32(buffer[0:]))
	bid := int64(binary.LittleEndian.Uint64(buffer[4:]))
	ask := int64(binary.LittleEndian.Uint64(buffer[12:]))
	sequence := int64(binary.LittleEndian.Uint64(buffer[20:]))
	publishedAt := int64(binary.LittleEndian.Uint64(buffer[28:]))

	latency := time.Now().UnixNano() - publishedAt
	if latency < 0 {
		s.negatives++
	} else {
		s.counts[min(latency/bucketNs, buckets-1)]++
		s.maxNs = max(s.maxNs, latency)
	}

	if s.firstSequence < 0 {
		s.firstSequence = sequence
	} else if sequence != s.lastSequence+1 {
		s.gaps++
	}
	s.lastSequence = sequence
	s.consumed++
	s.priceSum += bid + ask + instrumentID
}

func (s *consumerState) percentile(total int64, fraction float64) float64 {
	if total == 0 {
		return 0
	}
	target := max(int64(float64(total)*fraction), 1)
	var seen int64
	for index, count := range s.counts {
		seen += count
		if seen >= target {
			return (float64(index)*bucketNs + bucketNs/2.0) / 1000.0
		}
	}
	return 0
}

func (s *consumerState) report(label string) {
	var total int64
	var weighted float64
	for index, count := range s.counts {
		total += count
		weighted += float64(count) * (float64(index)*bucketNs + bucketNs/2.0)
	}
	mean := 0.0
	if total > 0 {
		mean = weighted / float64(total) / 1000.0
	}

	fmt.Fprintf(os.Stderr, "[consumer-%s] price_sum=%d\n", label, s.priceSum)
	fmt.Printf("{\"role\":\"consumer\",\"client\":\"%s\",\"consumed\":%d,\"first_sequence\":%d,"+
		"\"last_sequence\":%d,\"sequence_span\":%d,\"gaps\":%d,\"p50_us\":%.3f,\"p90_us\":%.3f,"+
		"\"p99_us\":%.3f,\"p999_us\":%.3f,\"max_us\":%.3f,\"mean_us\":%.3f,\"negative_latencies\":%d}\n",
		label, s.consumed, s.firstSequence, s.lastSequence, s.lastSequence-s.firstSequence+1, s.gaps,
		s.percentile(total, 0.5), s.percentile(total, 0.9), s.percentile(total, 0.99),
		s.percentile(total, 0.999), float64(s.maxNs)/1000.0, mean, s.negatives)
}
