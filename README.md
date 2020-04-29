# gobulk

gobulk is a tool to DB replication.

This tool can be used to create database for data analysis, staging environment.

When you run it, all schema information and data will be synced.

If you want to add options, write options in yaml files.

## Install

```
go get github.com/yuta17/gobulk
```

## Examples

[example](https://github.com/yuta17/gobulk/blob/master/example/main.go)

## Todo

- [x] Sync data from input database to output database.
- [ ] Masking data in specified column.
- [ ] Automatically follow column changes of input database.
- [ ] Support multiple DBMS client.
