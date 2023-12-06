// File: main.go
package main

import (
    "sync"
	"SDK-GO/consumer"
	"SDK-GO/producer"
)
func main() {
    var wg sync.WaitGroup
    wg.Add(2)

	go producer.Producer()
	go consumer.Consusmer()

    wg.Wait()
}