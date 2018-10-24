package main

import (
	"context"
	"fmt"
	"github.com/castaneai/spadmin"
	"log"
	"os"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Usage: <SPANNER PROJECT ID> <SPANNER INSTANCE ID> <SPANNER DATABASE NAME>")
		return
	}

	projectID := os.Args[1]
	instanceID := os.Args[2]
	databaseName := os.Args[3]

	if err := dropDatabase(projectID, instanceID, databaseName); err != nil {
		log.Fatalln(err)
	}
	fmt.Printf(`db %s successfully dropped:
https://console.cloud.google.com/spanner/instances/%s/databases?project=%s
`, databaseName, instanceID, projectID)
}

func dropDatabase(projectID, instanceID, databaseName string) error {
	dsn := fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID)

	admin, err := spadmin.NewClient(dsn)
	if err != nil {
		return err
	}

	ctx := context.Background()
	if err := admin.DropDatabase(ctx, databaseName); err != nil {
		return err
	}
	return nil
}