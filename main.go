package main

import (
	"bgptools/core/helpers"
	"log"
	"os"
)

func main() {

	logName := "/var/log/bgptools-combine/bgptools-combine.log"
	logFile, err := os.OpenFile(logName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Println(err)
	}
	log.SetOutput(logFile)
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	defer logFile.Close()

	helpers.Execute()

}
