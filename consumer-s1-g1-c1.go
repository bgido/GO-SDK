package main

import (
    "fmt"
    "context"
	"go-sdk/config"
    "os"
    "time"
    "github.com/memphisdev/memphis.go"
)

func main() {
	conn, err := memphis.Connect(
        config.Host, 
        config.User, 
        memphis.Password(config.Pass), 
        memphis.AccountId(config.AccountID),
	)
	
	if err != nil {
        os.Exit(1)
    }
    defer conn.Close()

    consumer, err := conn.CreateConsumer(
        "eventlog-1", 
        "consumer-GO-Barak-1",
        memphis.ConsumerGroup("Gido-1"), 
        memphis.PullInterval(100*time.Millisecond)
        memphis.BatchSize(1000 int), // defaults to 10
        memphis.BatchMaxWaitTime(5000*time.Millisecond)) // defaults to 5 seconds, has to be at least 1 ms

    if err != nil {
        fmt.Printf("Consumer creation failed: %v", err)
        os.Exit(1)
    }

    handler := func(msgs []*memphis.Msg, err error, ctx context.Context) {
        if err != nil {
            fmt.Printf("Fetch failed: %v", err)
            return
        }

        for _, msg := range msgs {
            fmt.Println(string(msg.Data()))
            msg.Ack()
        }
    }

    ctx := context.Background()
	ctx = context.WithValue(ctx, "key", "value")
	consumer.SetContext(ctx)
    consumer.Consume(handler)

    // The program will close the connection after 30 seconds,
    // the message handler may be called after the connection closed
    // so the handler may receive a timeout error
    time.Sleep(30 * time.Second)
}
