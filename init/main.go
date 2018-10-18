package main

import (
	"github.com/castaneai/spadmin"
	"os"
	"context"
	"fmt"
	"log"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Usage: <SPANNER PROJECT ID> <SPANNER INSTANCE ID> <SPANNER DATABASE NAME>")
		return
	}

	projectID := os.Args[1]
	instanceID := os.Args[2]
	databaseName := os.Args[3]

	if err := createDatabase(projectID, instanceID, databaseName); err != nil {
		log.Fatalln(err)
	}
}

func createDatabase(projectID, instanceID, databaseName string) error {
	dsn := fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID)

	admin, err := spadmin.NewClient(dsn)
	if err != nil {
		return err
	}

	ctx := context.Background()
	return admin.CreateDatabase(ctx, databaseName, []string{
		`
		CREATE TABLE UserInfo (
			ID STRING(36) NOT NULL,
			Name STRING(255) NOT NULL,
			Rank INT64 NOT NULL
		) PRIMARY KEY (ID)
		`,
		`CREATE INDEX UserInfoRank ON UserInfo(Rank)`,
	})
}
