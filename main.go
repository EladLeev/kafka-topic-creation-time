package main

import (
	"flag"
	"log"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type config struct {
	zkString string
	parallel int
}

// Config to use
var (
	Config = &config{}
	zkPath = "/brokers/topics"
	wg     sync.WaitGroup
)

func init() {
	flag.StringVar(&Config.zkString, "zookeeper", "localhost", "The Zookeeper path")
	flag.IntVar(&Config.parallel, "parallel", 50, "Bound parallelism")
	flag.Parse()
}

func getChildren(c *zk.Conn, path string) []string {
	// Use `Children` to get a slice of strings with all the children of provided path.
	data, _, err := c.Children(zkPath)
	if err != nil {
		panic(err)
	}
	return data
}

func main() {
	log.Printf("Going to connect to: %v, with parallel: %d\n", Config.zkString, Config.parallel)
	zk, _, err := zk.Connect([]string{Config.zkString}, time.Second)
	defer zk.Close()
	if err != nil {
		log.Fatalf("Unable to open connection to %v\n%v", Config.zkString, err)
	}
	topicList := getChildren(zk, zkPath)
	topicMeta := make(map[string]time.Time)
	// Block the amount of Goroutines
	// https://blog.golang.org/pipelines
	blocker := make(chan struct{}, Config.parallel)

	// For each one of the topics, get the creation time
	for _, topic := range topicList {
		wg.Add(1)
		blocker <- struct{}{}
		go func(topicName string) {
			defer wg.Done()
			_, topicStat, err := zk.Get(zkPath + "/" + topicName)
			if err != nil {
				return
			}
			topicMeta[topicName] = time.Unix(0, topicStat.Ctime*int64(time.Millisecond))
			<-blocker
		}(topic)
	}
	wg.Wait()
	log.Printf("map:%v", topicMeta)
}
