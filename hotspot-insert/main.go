package main

import (
	"cloud.google.com/go/spanner"
	"context"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"github.com/montanaflynn/stats"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const iterationCount = 1000

func main() {
	runtime.GOMAXPROCS(0)
	if len(os.Args) < 4 {
		fmt.Printf("Usage: <SPANNER PROJECT ID> <SPANNER INSTANCE ID> <SPANNER DATABASE NAME> <GOROUTINE_COUNT>")
		return
	}
	isShard := flag.Bool("shard", false, "if true, insert random shardNo")
	flag.Parse()
	fmt.Printf("set random shardNo: %v\n", *isShard)

	projectID := flag.Arg(0)
	instanceID := flag.Arg(1)
	databaseName := flag.Arg(2)
	n, err := strconv.Atoi(flag.Arg(3))
	if err != nil {
		log.Fatalln(err)
	}
	rand.Seed(time.Now().UnixNano())

	ctx := context.Background()
	dsn := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseName)
	client, err := spanner.NewClient(ctx, dsn)
	if err != nil {
		log.Fatalln(err)
	}

	writeTimeChan := make(chan time.Duration, n*iterationCount)
	readTimeChan := make(chan time.Duration, n*iterationCount)
	idChan := make(chan string)
	doneChan := make(chan struct{})

	go logger(readTimeChan, writeTimeChan, doneChan)
	go readWorker(ctx, client, idChan, readTimeChan)

	wg := new(sync.WaitGroup)
	fmt.Println("start...")
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := run(ctx, client, writeTimeChan, *isShard, idChan); err != nil {
				log.Fatalln(err)
			}
		}(i)
	}
	fmt.Println("waiting...")
	wg.Wait()
	close(writeTimeChan)
	close(readTimeChan)
	close(doneChan)
	fmt.Println("complete!")
}

func run(ctx context.Context, client *spanner.Client, wch chan time.Duration, isShard bool, idCh chan string) error {
	table := "UserInfo"
	cols := []string{"ID", "Name", "Rank", "ShardNo"}
	shardNo := 0
	if isShard {
		shardNo = rand.Intn(100)
	}

	for i := 0; i < iterationCount; i++ {
		uid := uuid.Must(uuid.NewRandom()).String()
		idCh <- uid
		start := time.Now()
		if _, err := client.ReadWriteTransaction(ctx, func(tctx context.Context, tx *spanner.ReadWriteTransaction) error {
			var muts []*spanner.Mutation
			muts = append(muts, spanner.Insert(table, cols, []interface{}{uid, "もぷ", 1, shardNo}))
			if err := tx.BufferWrite(muts); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
		wch <- time.Since(start)
	}
	return nil
}

func readWorker(ctx context.Context, client *spanner.Client, idch chan string, rch chan time.Duration) {
	var ids []string
	cols := []string{"ID", "Name", "Rank", "ShardNo"}
	for id := range idch {
		ids = append(ids, id)
		if len(ids) > 0 {
			i := rand.Intn(len(ids))
			key := spanner.Key{ids[i]}
			start := time.Now()
			client.Single().ReadRow(ctx, "UserInfo", key, cols)
			rch <- time.Since(start)
		}
	}
}

func logger(rch, wch chan time.Duration, doneCh chan struct{}) {
	fp, err := os.Create(time.Now().Format("20060102150405_") + "exectime.csv")
	if err != nil {
		log.Fatalln(err)
	}
	fp.WriteString("logged_at,write_avg,write_max,write_min,write_p90,write_p95,write_med,read_avg,read_max,read_min,read_p90,read_p95,read_med,write_count,read_count\n")
	ws := []float64{}
	rs := []float64{}
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <- ticker.C:
			if len(ws) > 1 && len(rs) > 1 {
				wstat := GetStat(ws)
				rstat := GetStat(rs)
				columns := []string{
					time.Now().Format("15:04:05"),
					fmt.Sprintf("%f", float64(time.Duration(wstat.Avg))/float64(time.Millisecond)),
					fmt.Sprintf("%f", float64(time.Duration(wstat.Max))/float64(time.Millisecond)),
					fmt.Sprintf("%f", float64(time.Duration(wstat.Min))/float64(time.Millisecond)),
					fmt.Sprintf("%f", float64(time.Duration(wstat.P90))/float64(time.Millisecond)),
					fmt.Sprintf("%f", float64(time.Duration(wstat.P95))/float64(time.Millisecond)),
					fmt.Sprintf("%f", float64(time.Duration(wstat.Med))/float64(time.Millisecond)),
					fmt.Sprintf("%f", float64(time.Duration(rstat.Avg))/float64(time.Millisecond)),
					fmt.Sprintf("%f", float64(time.Duration(rstat.Max))/float64(time.Millisecond)),
					fmt.Sprintf("%f", float64(time.Duration(rstat.Min))/float64(time.Millisecond)),
					fmt.Sprintf("%f", float64(time.Duration(rstat.P90))/float64(time.Millisecond)),
					fmt.Sprintf("%f", float64(time.Duration(rstat.P95))/float64(time.Millisecond)),
					fmt.Sprintf("%f", float64(time.Duration(rstat.Med))/float64(time.Millisecond)),
					strconv.Itoa(len(ws)),
					strconv.Itoa(len(rs)),
				}
				line := strings.Join(columns, ",")
				fp.WriteString(line + "\n")
				ws = []float64{}
				rs = []float64{}
			}
		case r := <- rch:
			rs = append(rs, float64(r))
		case w := <- wch:
			ws = append(ws, float64(w))
		case <-doneCh:
			return
		}
	}
}

type StatInfo struct {
	Avg float64
	Max float64
	Min float64
	P90 float64
	P95 float64
	Med float64
}

func GetStat(fs []float64) StatInfo {
	avg, _ := stats.Mean(fs)
	max, _ := stats.Max(fs)
	min, _ := stats.Min(fs)
	p90, _ := stats.Percentile(fs, 90)
	p95, _ := stats.Percentile(fs, 95)
	med, _ := stats.Percentile(fs, 50)
	return StatInfo{
		Avg: avg,
		Max: max,
		Min: min,
		P90: p90,
		P95: p95,
		Med: med,
	}
}
