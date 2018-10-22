package main

import (
	"os"
	"fmt"
	"cloud.google.com/go/spanner"
	"context"
	"log"
	"github.com/google/uuid"
	"strconv"
	"time"
	"k8s.io/apimachinery/pkg/util/rand"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Printf("Usage: <SPANNER PROJECT ID> <SPANNER INSTANCE ID> <SPANNER DATABASE NAME> <SHARD_COUNT> <GOROUTINE_COUNT>")
		return
	}

	projectID := os.Args[1]
	instanceID := os.Args[2]
	databaseName := os.Args[3]
	shardNum, err := strconv.Atoi(os.Args[4])
	if err != nil {
		log.Fatalln(err)
	}
	n, err := strconv.Atoi(os.Args[5])
	if err != nil {
		log.Fatalln(err)
	}

	for i := 0; i < n; i++ {
		go func() {
			ctx := context.Background()
			if err := run(ctx, projectID, instanceID, databaseName, shardNum); err != nil {
				log.Fatalln(err)
			}
		}()
	}
	fmt.Printf("start...")

	for {
		time.Sleep(1 * time.Second)
	}
}

func run(ctx context.Context, projectID, instanceID, databaseName string, shardNum int) error {
	dsn := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseName)

	client, err := spanner.NewClient(ctx, dsn)
	if err != nil {
		return err
	}

	for {
		if _, err := client.ReadWriteTransaction(ctx, func(tctx context.Context, tx *spanner.ReadWriteTransaction) error {
			uid := uuid.Must(uuid.NewRandom()).String()
			shardNo := rand.IntnRange(1, shardNum)
			stmt := spanner.NewStatement(fmt.Sprintf("INSERT INTO UserInfo (ID, Name, Rank, ShardNo) VALUES ('%s', '%s', %d, %d)", uid, "test", 1, shardNo))
			_, err := tx.Update(tctx, stmt)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
	}
}