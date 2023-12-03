package main

import (
	"fmt"
	"go-sdk/config"
	"os"
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
        fmt.Printf("Connection failed: %v", err)
        os.Exit(1)
    }
    defer conn.Close()

    p, err := conn.CreateProducer("eventlog-1", "producer-GO-Barak-1")
    if err != nil {
        fmt.Printf("Producer failed: %v", err)
        os.Exit(1)
    }
    
    hdrs := memphis.Headers{}
    hdrs.New()
    err = hdrs.Add("fname", "Barak")
	if err != nil {
		fmt.Printf("Header failed: %v", err)
		os.Exit(1)
	}
	err = hdrs.Add("lname", "Gido")
	if err != nil {
		fmt.Printf("Header failed: %v", err)
		os.Exit(1)
	}
	err = hdrs.Add("age", "33")
	if err != nil {
		fmt.Printf("Header failed: %v", err)
		os.Exit(1)
	}
	err = hdrs.Add("city", "Tel Aviv")
	if err != nil {
		fmt.Printf("Header failed: %v", err)
		os.Exit(1)
	}
    	msg := make(map[string]interface{})
        msg["fname"] = "Barak"
        msg["lname"] = "Gido"
    
    counter := 0

	// Infinite loop
	for {
    err = p.Produce(msg, memphis.MsgHeaders(hdrs), memphis.AsyncProduce())

    if err != nil {
        fmt.Printf("Produce failed: %v", err)
        os.Exit(1)
        }

    counter++
    }
}
        