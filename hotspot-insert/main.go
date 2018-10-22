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
)

func main() {
	if len(os.Args) < 4 {
		fmt.Printf("Usage: <SPANNER PROJECT ID> <SPANNER INSTANCE ID> <SPANNER DATABASE NAME> <GOROUTINE_COUNT>")
		return
	}

	projectID := os.Args[1]
	instanceID := os.Args[2]
	databaseName := os.Args[3]
	n, err := strconv.Atoi(os.Args[4])
	if err != nil {
		log.Fatalln(err)
	}

	for i := 0; i < n; i++ {
		go func() {
			ctx := context.Background()
			if err := run(ctx, projectID, instanceID, databaseName); err != nil {
				log.Fatalln(err)
			}
		}()
	}
	fmt.Printf("start...")

	for {
		time.Sleep(1 * time.Second)
	}
}

func run(ctx context.Context, projectID, instanceID, databaseName string) error {
	dsn := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseName)

	client, err := spanner.NewClient(ctx, dsn)
	if err != nil {
		return err
	}

	for {
		if _, err := client.ReadWriteTransaction(ctx, func(tctx context.Context, tx *spanner.ReadWriteTransaction) error {
			uid := uuid.Must(uuid.NewRandom()).String()
			stmt := spanner.NewStatement(fmt.Sprintf("INSERT INTO UserInfo (ID, Name, Rank) VALUES ('%s', '%s', %d)", uid, "test", 1))
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