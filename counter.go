package main

import (
	"encoding/binary"
	"flag"
	"github.com/go-redis/redis"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

type Storage map[uint32]uint32
type DoneChannel chan struct{}
type DataChannel chan uint32

type RedisTask struct {
	client  *redis.Client
	ticker  *time.Ticker
	storage Storage
	data    DataChannel
	done    DoneChannel
}

func processConnection(c net.Conn, dataChannel DataChannel) {
	for {
		buf := make([]byte, 4)
		nr, err := c.Read(buf)
		if err != nil {
			return
		}

		data := buf[0:nr]
		projectId := binary.LittleEndian.Uint32(data)
		log.Println("Visited project id", projectId)

		dataChannel <- projectId
	}
}

func (t *RedisTask) Save() {
	if len(t.storage) == 0 {
		return
	}

	pairs := make([]string, len(t.storage)*2)
	var n uint32 = 0

	for projectId, timestamp := range t.storage {
		pairs[n] = strconv.Itoa(int(projectId))
		pairs[n+1] = strconv.Itoa(int(timestamp))
		n += 2
		delete(t.storage, projectId)
	}

	log.Println("Saving data to redis", pairs)
	t.client.MSet(pairs)
}

func (t *RedisTask) Run(flushInterval int) {
	t.storage = make(Storage)
	t.ticker = time.NewTicker(time.Second * time.Duration(flushInterval))
	defer t.ticker.Stop()

	for {
		select {
		case projectId := <-t.data:
			t.storage[projectId] = uint32(time.Now().Unix())
		case <-t.ticker.C:
			t.Save()
		case <-t.done:
			t.Save()
			os.Exit(1)
		}
	}
}

func main() {
	socketNamePtr := flag.String("socket", "/tmp/counter.sock", "Socket path")
	redisAddrPtr := flag.String("redis-addr", "localhost:6379", "Redis addr")
	redisDbPtr := flag.Int("redis-db", 5, "Redis database number")
	flushIntervalPtr := flag.Int("flush-interval", 5, "Flush interval in seconds")
	flag.Parse()
	socketName := *socketNamePtr

	// Connect to Redis
	client := redis.NewClient(&redis.Options{
		Addr: *redisAddrPtr,
		DB:   *redisDbPtr,
	})
	defer client.Close()
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

	// Listen to CTRL+C and SIGTERM
	doneChannel := make(DoneChannel)
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		sig := <-c
		log.Printf("Got %s signal. Aborting...\n", sig)
		close(doneChannel)
	}()

	// Create data channel for interchange between goroutines
	dataChannel := make(DataChannel)

	// Init Redis task
	redisTask := &RedisTask{
		client: client,
		data:   dataChannel,
		done:   doneChannel,
	}
	go redisTask.Run(*flushIntervalPtr)

	for {
		con, err := l.Accept()
		if err != nil {
			log.Fatal("accept error:", err)
		}

		go processConnection(con, dataChannel)
	}
}
