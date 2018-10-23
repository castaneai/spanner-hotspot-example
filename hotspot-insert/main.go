package main

import (
	"cloud.google.com/go/spanner"
	"context"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

const iterationCount = 5000

func main() {
	if len(os.Args) < 4 {
		fmt.Printf("Usage: <SPANNER PROJECT ID> <SPANNER INSTANCE ID> <SPANNER DATABASE NAME> <GOROUTINE_COUNT>")
		return
	}
	isShard := flag.Bool("shard", false, "if true, insert random shardNo")
	flag.Parse()
	fmt.Printf("set random shardNo: %v\n", *isShard)
	fmt.Println(flag.NArg(), flag.NFlag())

	projectID := flag.Arg(0)
	instanceID := flag.Arg(1)
	databaseName := flag.Arg(2)
	n, err := strconv.Atoi(flag.Arg(3))
	if err != nil {
		log.Fatalln(err)
	}
	rand.Seed(time.Now().UnixNano())

	timeChan := make(chan time.Duration, n*iterationCount)

	wg := new(sync.WaitGroup)
	fmt.Println("start...")
	for i := 0; i < n; i++ {
		wg.Add(1)
		fmt.Printf("goroutine %d start\n", i)
		go func(i int) {
			defer wg.Done()
			ctx := context.Background()
			if err := run(ctx, projectID, instanceID, databaseName, timeChan, i, *isShard); err != nil {
				log.Fatalln(err)
			}
		}(i)
	}
	fmt.Println("waiting...")
	wg.Wait()
	fmt.Println("complete!")
	close(timeChan)
	fmt.Printf("average time: %v\n", DurationAvg(timeChan).String())
}

// 平均を返す
func DurationAvg(d chan time.Duration) time.Duration {
	var sum time.Duration
	cnt := len(d)
	for v := range d {
		sum += v
	}
	return sum / time.Duration(cnt)
}

func run(ctx context.Context, projectID, instanceID, databaseName string, tch chan time.Duration, goroutineId int, isShard bool) error {
	dsn := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseName)

	client, err := spanner.NewClient(ctx, dsn)
	if err != nil {
		return err
	}

	table := "UserInfo"
	cols := []string{"ID", "Name", "Rank", "ShardNo"}
	shardNo := 0
	if isShard {
		shardNo = rand.Intn(100)
	}

	for i := 0; i < iterationCount; i++ {
		start := time.Now()
		if _, err := client.ReadWriteTransaction(ctx, func(tctx context.Context, tx *spanner.ReadWriteTransaction) error {
			uid := uuid.Must(uuid.NewRandom()).String()
			//stmt := spanner.NewStatement(fmt.Sprintf("INSERT INTO UserInfo (ID, Name, Rank) VALUES ('%s', '%s', %d)", uid, "test", 1))
			//tx.QueryWithStats(tctx, stmt)
			m := spanner.Insert(table, cols, []interface{}{uid, "もぷ", 1, shardNo})
			if err := tx.BufferWrite([]*spanner.Mutation{m}); err != nil {
				return err
			}
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
		tch <- time.Since(start)
	}
	fmt.Printf("goroutine %d finished\n", goroutineId)
	return nil
}
