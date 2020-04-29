package main

import (
	"github.com/prometheus/common/log"
	"github.com/yuta17/gobulk"
)

func main() {
	inputURL := "root@tcp(127.0.0.1:3306)/YOUR_INPUT_DATABASE_NAME"
	outputURL := "root@tcp(127.0.0.1:3306)/YOUR_OUTPUT_DATABASE_NAME"
	client, err := gobulk.NewClient("mysql", inputURL, outputURL)
	if err != nil {
		log.Errorln(err)
	}
	err = client.Sync()
	if err != nil {
		log.Errorln(err)
	}
}
