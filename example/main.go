package main

import (
	"github.com/prometheus/common/log"
	"github.com/yuta17/gobulk"
)

func main() {
	inputUrl := "root@tcp(127.0.0.1:3306)/campfire_development"
	outputUrl := "root@tcp(127.0.0.1:3306)/campfire_development_copy"
	client := gobulk.NewClient("mysql", inputUrl, outputUrl)
	err := client.Sync()
	if err != nil {
		log.Errorln(err)
	}
}
