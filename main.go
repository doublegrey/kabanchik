//nolint:forbidigo,gomnd
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	topic       = flag.String("topic", "", "topic to produce to or consume from")

	recordBytes   = flag.Int("record-bytes", 100, "bytes per record value (producing)")
	compression   = flag.String("compression", "none", "compression algorithm to use (none,gzip,snappy,lz4,zstd, for producing)")
	batchMaxBytes = flag.Int("batch-max-bytes", 1000000, "the maximum batch size to allow per-partition (must be less than Kafka's max.message.bytes, producing)")

	logLevel = flag.String("log-level", "", "if non-empty, use a basic logger with this log level (debug, info, warn, error)")

	consume = flag.Bool("consume", false, "if true, consume rather than produce")
	group   = flag.String("group", "", "if non-empty, group to use for consuming rather than direct partition consuming (consuming)")

	dialTLS      = flag.Bool("tls", false, "if true, use tls")
	saslMethod   = flag.String("sasl-method", "", "if non-empty, sasl method to use (supports plain, scram-sha-256, scram-sha-512, aws_msk_iam)")
	saslUsername = flag.String("username", "", "if non-empty, username to use for sasl")
	saslPassword = flag.String("password", "", "if non-empty, password to use for sasl ")

	rateRecs  int64
	rateBytes int64

	pool = sync.Pool{New: func() any { return kgo.SliceRecord(make([]byte, *recordBytes)) }}
)

func displayStats() {
	for range time.Tick(time.Second) {
		recs := atomic.SwapInt64(&rateRecs, 0)
		bytes := atomic.SwapInt64(&rateBytes, 0)
		fmt.Printf("%0.2f MiB/s; %0.2fk records/s\n", float64(bytes)/(1024*1024), float64(recs)/1000)
	}
}

func die(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func check(err error, msg string, args ...any) {
	if err != nil {
		die(msg, args...)
	}
}

func main() {
	flag.Parse()

	if *recordBytes <= 0 {
		die("record bytes must be larger than zero")
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		kgo.DefaultProduceTopic(*topic),
		kgo.MaxBufferedRecords(250<<20 / *recordBytes + 1),
		kgo.MaxConcurrentFetches(3),
		kgo.FetchMaxBytes(5 << 20),
		kgo.ProducerBatchMaxBytes(int32(*batchMaxBytes)),
	}

	if *consume {
		opts = append(opts, kgo.ConsumeTopics(*topic))
		if *group != "" {
			opts = append(opts, kgo.ConsumerGroup(*group))
		}
	}

	switch strings.ToLower(*logLevel) {
	case "":
	case "debug":
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)))
	case "info":
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)))
	case "warn":
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelWarn, nil)))
	case "error":
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelError, nil)))
	default:
		die("unrecognized log level %s", *logLevel)
	}

	switch strings.ToLower(*compression) {
	case "", "none":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.NoCompression()))
	case "gzip":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.GzipCompression()))
	case "snappy":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.SnappyCompression()))
	case "lz4":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.Lz4Compression()))
	case "zstd":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.ZstdCompression()))
	default:
		die("unrecognized compression %s", *compression)
	}

	if *dialTLS {
		opts = append(opts, kgo.DialTLSConfig(new(tls.Config)))
	}

	if *saslMethod != "" || *saslUsername != "" || *saslPassword != "" {
		if *saslMethod == "" || *saslUsername == "" || *saslPassword == "" {
			die("not all sasl params are specified: method: %t | username: %t | password: %t", *saslMethod != "", *saslUsername != "", *saslPassword != "")
		}

		method := strings.ToLower(*saslMethod)
		method = strings.ReplaceAll(method, "-", "")
		method = strings.ReplaceAll(method, "_", "")

		switch method {
		case "plain":
			opts = append(opts, kgo.SASL(plain.Auth{
				User: *saslUsername,
				Pass: *saslPassword,
			}.AsMechanism()))
		case "scramsha256":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: *saslUsername,
				Pass: *saslPassword,
			}.AsSha256Mechanism()))
		case "scramsha512":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: *saslUsername,
				Pass: *saslPassword,
			}.AsSha512Mechanism()))
		case "awsmskiam":
			opts = append(opts, kgo.SASL(aws.Auth{
				AccessKey: *saslUsername,
				SecretKey: *saslPassword,
			}.AsManagedStreamingIAMMechanism()))
		default:
			die("unrecognized sasl option %s", *saslMethod)
		}
	}

	cl, err := kgo.NewClient(opts...)
	check(err, "unable to initialize client: %v", err)

	go displayStats()

	if *consume {
		for {
			fetches := cl.PollFetches(context.Background())
			fetches.EachError(func(t string, p int32, err error) {
				check(err, "topic %s partition %d had error: %v", t, p, err)
			})
			var recs int64
			var bytes int64
			fetches.EachRecord(func(r *kgo.Record) {
				recs++
				bytes += int64(len(r.Value))
			})
			atomic.AddInt64(&rateRecs, recs)
			atomic.AddInt64(&rateBytes, bytes)
		}
	} else {
		var num int64

		for {
			cl.Produce(context.Background(), newRecord(num), func(r *kgo.Record, err error) {
				pool.Put(r)
				check(err, "produce error: %v", err)
				atomic.AddInt64(&rateRecs, 1)
				atomic.AddInt64(&rateBytes, int64(*recordBytes))
			})
			num++
		}
	}
}

func newRecord(num int64) *kgo.Record {
	r := pool.Get().(*kgo.Record) //nolint:errcheck,forcetypeassert
	formatValue(num, r.Value)

	return r
}

func formatValue(num int64, v []byte) {
	var buf [20]byte // max int64 takes 19 bytes, then we add a space
	b := strconv.AppendInt(buf[:0], num, 10)
	b = append(b, ' ')

	n := copy(v, b)
	for n != len(v) {
		n += copy(v[n:], b)
	}
}
