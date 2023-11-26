package turboProducing
import (
    "context"
    "encoding/json"
    "fmt"
    "math/rand"
    "os"
    "strconv"
    "time"
    "github.com/memphisdev/memphis.go"
)
type AcidS struct {
    Acid int `json:"acid"`
}
func main() {
    // getting environment variables
    hostname := "aws-eu-central-1.cloud-staging.memphis.dev"
    username := "shohamr"
    password := "Shoham123456!"
    accountIdString := "223672095"
    counsumerCountStr := "3"
    producerCount := "3"
    fileSizeParam := "100" // Specify the desired file size in KB int64(1024 * NUM)
    if hostname == "" || username == "" || password == "" || accountIdString == "" || fileSizeParam == "" || counsumerCountStr == "" || producerCount == "" {
        fmt.Printf("Error one of the variables is empty")
        os.Exit(1)
    }
    // string conversion
    consumerCount, err := strconv.Atoi(counsumerCountStr)
    if err != nil {
        fmt.Printf("Error during conversion counsumerCountStr file size: %v", err.Error())
        os.Exit(1)
    }
    accountId, err := strconv.Atoi(accountIdString)
    if err != nil {
        fmt.Printf("Error during conversion accountID file size: %v", err.Error())
        os.Exit(1)
    }
    pCount, err := strconv.Atoi(producerCount)
    if err != nil {
        fmt.Printf("Error during conversion producerCount file size: %v", err.Error())
        os.Exit(1)
    }
    fileSize, err := strconv.ParseInt(fileSizeParam, 10, 64)
    if err != nil {
        fmt.Printf("Error during conversion file size: %v", err.Error())
        os.Exit(1)
    }
    fileSize = fileSize * 1024
    // creating the station
    stationName := generateString() // generate station name
    conn, err := memphis.Connect(hostname, username, memphis.Password(password), memphis.AccountId(accountId))
    if err != nil {
        fmt.Printf("error creating station connection: %v", err.Error())
        os.Exit(1)
    }
    _, err = conn.CreateStation(stationName,
        memphis.RetentionTypeOpt(0),
        memphis.RetentionVal(300),
        memphis.StorageTypeOpt(0),
        memphis.Replicas(3),
    )
    if err != nil {
        fmt.Printf("error creating station: %v", err.Error()) //
        os.Exit(1)
    } else {
        fmt.Println(fmt.Sprintf("station %v created", stationName))
    }
    conn.Close()
    // generate message
    jsonMessage, err := generateJSON(fileSize)
    if err != nil {
        fmt.Println("Error:", err)
        return
    }
    // creating memphis producers
    for i := 0; i < pCount; i++ {
        go func(hostname string, username string, password string, stationName string, fileSize int64, counter int, jsonMessage []byte) {
            conn, err := memphis.Connect(hostname, username, memphis.Password(password), memphis.AccountId(accountId))
            if err != nil {
                fmt.Printf("error creating producer connection: %v", err.Error())
                os.Exit(1)
            }
            defer conn.Close()
            pname := generateString()
            p, err := conn.CreateProducer(stationName, pname)
            if err != nil {
                fmt.Printf("error creating producer: %v", err)
                os.Exit(1)
            }
            for {
                err = p.Produce(jsonMessage)
                if err != nil {
                    fmt.Printf("Produce failed: %v", err)
                }
            }
        }(hostname, username, password, stationName, fileSize, i, jsonMessage)
        fmt.Println("Producer created")
    }
    // creating message handler
    handler := func(msgs []*memphis.Msg, err error, ctx context.Context) {
        if err != nil {
            fmt.Printf("falied fetching :( ")
        }
        for _, msg := range msgs {
            msg.Ack()
        }
    }
    cgName := generateString()
    // creating memphis consumers
    for i := 0; i < consumerCount; i++ {
        go func(hostname, username, password, stationName, cgName string, handler memphis.ConsumeHandler) {
            conn, err := memphis.Connect(hostname, username, memphis.Password(password), memphis.AccountId(accountId))
            if err != nil {
                fmt.Printf("Error connecting consumer to memphis: %v", err.Error())
                os.Exit(1)
            }
            consumerName := generateString()
            consumer, err := conn.CreateConsumer(stationName, consumerName, memphis.ConsumerGroup(cgName))
            if err != nil {
                fmt.Printf("Error creating consumer: %v", err.Error())
            }
            consumer.Consume(handler)
        }(hostname, username, password, stationName, cgName, handler)
        fmt.Println("Consumer created")
    }
    fmt.Println("working...")
    time.Sleep(time.Duration(1<<63 - 1))
    fmt.Printf("finished")
}
func generateJSON(fileSize int64) ([]byte, error) {
    data := generateRandomData(fileSize)
    jsonData, err := json.Marshal(data)
    if err != nil {
        return nil, err
    }
    return jsonData, nil
}
func generateRandomData(fileSize int64) []string {
    var data []string
    currentSize := int64(0)
    for currentSize < fileSize {
        item := generateString()
        data = append(data, item)
        currentSize += int64(len(item))
    }
    return data
}
func generateString() string {
    const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    const itemSize = 10
    rand.Seed(time.Now().UnixNano())
    result := make([]byte, itemSize)
    for i := range result {
        result[i] = charset[rand.Intn(len(charset))]
    }
    return string(result)
}