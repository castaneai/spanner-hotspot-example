package main

import (
	"os"
	"fmt"
	"cloud.google.com/go/spanner"
	"context"
	"log"
	"github.com/google/uuid"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Usage: <SPANNER PROJECT ID> <SPANNER INSTANCE ID> <SPANNER DATABASE NAME>")
		return
	}

	projectID := os.Args[1]
	instanceID := os.Args[2]
	databaseName := os.Args[3]

	ctx := context.Background()
	if err := run(ctx, projectID, instanceID, databaseName); err != nil {
		log.Fatalln(err)
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