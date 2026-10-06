package benchkit

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/andrewwormald/aergo"
)

const TickLength = 28

const serviceInterval = 1024

type Client struct {
	Aeron     *aergo.Aeron
	sinceRun  int
	imageSeen bool
}

func Connect() (*Client, error) {
	aeron, err := aergo.Connect(aergo.WithDir(AeronDir()))
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", AeronDir(), err)
	}
	return &Client{Aeron: aeron}, nil
}

func (c *Client) Service() {
	c.sinceRun++
	if c.sinceRun >= serviceInterval {
		c.sinceRun = 0
		c.Aeron.DoWork()
	}
}

func (c *Client) Idle() {
	c.sinceRun = 0
	c.Aeron.DoWork()
}

func (c *Client) AfterPoll(received int) {
	switch {
	case received > 0:
		c.imageSeen = true
		c.Service()
	case c.imageSeen:
		c.Service()
	default:
		c.Idle()
	}
}

func (c *Client) AwaitConnected(pub *aergo.Publication, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for !pub.IsConnected() {
		if time.Now().After(deadline) {
			return fmt.Errorf("publication not connected within %s", timeout)
		}
		c.Idle()
	}
	return nil
}

func AeronDir() string {
	return EnvOr("AERON_DIR", "/tmp/ae_drv")
}

func TermLength() string {
	return EnvOr("BENCH_TERM_LENGTH", "16777216")
}

func EnvOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func WriteTick(buf []byte, sequence int64) {
	if len(buf) < TickLength {
		return
	}
	bid := 100000 + sequence%1000
	binary.LittleEndian.PutUint32(buf[0:], uint32(sequence%100))
	binary.LittleEndian.PutUint64(buf[4:], uint64(bid))
	binary.LittleEndian.PutUint64(buf[12:], uint64(bid+10))
	binary.LittleEndian.PutUint64(buf[20:], uint64(sequence))
}

const PoolEntries = 4096

const poolSeed uint64 = 0x5EEDAE20

type Pool struct {
	entries [][]byte
}

func NewPool(size int) *Pool {
	total := PoolEntries * size
	stream := make([]byte, (total+7)/8*8)
	state := poolSeed
	for offset := 0; offset < len(stream); offset += 8 {
		state += 0x9E3779B97F4A7C15
		mixed := (state ^ (state >> 30)) * 0xBF58476D1CE4E5B9
		mixed = (mixed ^ (mixed >> 27)) * 0x94D049BB133111EB
		mixed ^= mixed >> 31
		binary.LittleEndian.PutUint64(stream[offset:], mixed)
	}
	pool := &Pool{entries: make([][]byte, PoolEntries)}
	for index := range pool.entries {
		pool.entries[index] = stream[index*size : (index+1)*size : (index+1)*size]
	}
	return pool
}

func (p *Pool) Entry(sequence int64) []byte {
	return p.entries[sequence%PoolEntries]
}

func (p *Pool) Message(sequence int64) []byte {
	entry := p.Entry(sequence)
	WriteTick(entry, sequence)
	return entry
}

func ReadTick(buf []byte) int64 {
	if len(buf) < TickLength {
		return 0
	}
	instrumentID := int64(binary.LittleEndian.Uint32(buf[0:]))
	bid := int64(binary.LittleEndian.Uint64(buf[4:]))
	ask := int64(binary.LittleEndian.Uint64(buf[12:]))
	sequence := int64(binary.LittleEndian.Uint64(buf[20:]))
	return bid + instrumentID + ask + sequence
}

func SortSamples(samples []int64) {
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
}

func PercentileMicros(sorted []int64, pct float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(pct / 100.0 * float64(len(sorted)))
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return float64(sorted[index]) / 1000.0
}

func MeanMicros(samples []int64) float64 {
	if len(samples) == 0 {
		return 0
	}
	var total float64
	for _, sample := range samples {
		total += float64(sample)
	}
	return total / float64(len(samples)) / 1000.0
}

func Fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
