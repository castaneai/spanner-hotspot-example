Cloud Spanner Hotspot Example
=================================

```sh
# create database
go run init/main.go <SPANNER_PROJECT_ID> <SPANNER_INSTANCE_ID> <SPANNER_DATABASE_NAME>

# run insert with hotspot
go run hotspot-insert/main.go <SPANNER_PROJECT_ID> <SPANNER_INSTANCE_ID> <SPANNER_DATABASE_NAME>
```
