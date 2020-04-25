# gobulk

gobulk is a tool to DB replication.

This tool can be used to create database for data analysis, staging environment.

When you run it, all schema information and data will be synchronized.

If you want to add options, write options in yaml files.

## Install

```
go get github.com/yuta17/gobulk
```

## Usage

```go
inputUrl := `YOUR INPUT DB URL`
outputUrl := `YOUR OUTPUT DB URL`
columnOptions := ioutil.ReadFile("./tables/table1.yml")

client := gobulk.NewClient(inputUrl, outputUrl)
client.SetColumnOptions(columnOptions)
client.Sync()
```


```yml
# ./tables/table1.yml
table:
  name: table1
  masking_columns:
    - column1
    - column2
```
