package main

import (
	"encoding/binary"
	"flag"
	"github.com/go-redis/redis"
	"log"
	"net"
	"os"
	"strconv"
	"time"
)

func processConnection(c net.Conn, storage map[uint32]uint32) {
	for {
		buf := make([]byte, 4)
		nr, err := c.Read(buf)
		if err != nil {
			return
		}

		data := buf[0:nr]
		projectId := binary.LittleEndian.Uint32(data)
		log.Println("Visited project id", projectId)

		storage[projectId] = uint32(time.Now().Unix())
	}
}

func saveDataToRedis(client *redis.Client, storage map[uint32]uint32) {
	if len(storage) > 0 {
		pairs := make([]string, len(storage)*2)
		var n uint32 = 0

		for projectId, timestamp := range storage {
			pairs[n] = strconv.Itoa(int(projectId))
			pairs[n+1] = strconv.Itoa(int(timestamp))
			n += 2
			delete(storage, projectId)
		}

		log.Println("Saving data to redis", pairs)
		client.MSet(pairs)
	}
}

func saveDataToRedisTicker(client *redis.Client, ticker *time.Ticker, storage map[uint32]uint32) {
	for range ticker.C {
		saveDataToRedis(client, storage)
	}
}

func main() {
	socketNamePtr := flag.String("socket", "/tmp/counter.sock", "Socket path")
	redisAddrPtr := flag.String("redis-addr", "localhost:6379", "Redis addr")
	redisDbPtr := flag.Int("redis-db", 5, "Redis database number")
	flushIntervalPtr := flag.Int("flush-interval", 5, "Flush interval in seconds")
	flag.Parse()
	socketName := *socketNamePtr

	storage := make(map[uint32]uint32)

	// Connect to Redis
	client := redis.NewClient(&redis.Options{
		Addr: *redisAddrPtr,
		DB:   *redisDbPtr,
	})
	log.Println("Connected to redis", *redisAddrPtr)

	// Listen to socket
	e := os.Remove(socketName)
	if e != nil {
		log.Println(e)
	}
	l, err := net.Listen("unix", socketName)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	defer l.Close()
	log.Println("Listening to socket", socketName)

	// Init timer
	timer := time.NewTicker(time.Second * time.Duration(*flushIntervalPtr))
	go saveDataToRedisTicker(client, timer, storage)
	defer timer.Stop()
	defer saveDataToRedis(client, storage)
	defer client.Close()

	for {
		con, err := l.Accept()
		if err != nil {
			log.Fatal("accept error:", err)
		}

		go processConnection(con, storage)
	}
}
