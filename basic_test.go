package pghub

import (
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

type Payload1 struct {
	A float64   `json:"a,omitempty"`
	B string    `json:"b,omitempty"`
	C []float64 `json:"c,omitempty"`
}

func Test1(t *testing.T) {
	pool, err1 := initPgxPool(newTestConfig())
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	SetLogger(logger)

	if err1 != nil {
		t.Fatal(err1)
	}

	config := &Config{}

	config.Logger = logger
	config.Pool = pool
	config.ConnectRetry = time.Second * 5

	hub, err2 := New("test", config)

	if err2 != nil {
		t.Fatal(err2)
	}

	err3 := hub.Start()

	if err3 != nil {
		t.Fatal(err3)
	}

	const consumerCount = 100
	const eventCount = 100
	cbchan1 := make(chan interface{})
	cbchan2 := make(chan interface{})
	cbchan3 := make(chan interface{})
	cbchan4 := make(chan interface{})
	var subscribes atomic.Uint32
	var events atomic.Uint32
	var consumer [consumerCount]*Consumer

	cb := func(event *Event) {
		if event.topic != "" {
			count := events.Add(1)

			//		logger.Info("received " + strconv.Itoa(int(count)) + "/" + strconv.Itoa(int(eventCount*consumerCount)*2))
			if count == eventCount*consumerCount {
				cbchan3 <- nil
			}

			if count == eventCount*consumerCount*2 {
				cbchan4 <- nil
			}
		} else {
			advisory := event.value.(*Advisory)
			if advisory.action == ActionSubscribe {
				count := subscribes.Add(1)

				if count == consumerCount*3 {
					cbchan1 <- nil
				}

				if count == consumerCount*4 {
					cbchan2 <- nil
				}
			}
		}
	}

	for i := 0; i < consumerCount; i++ {
		var err4 error
		consumer[i], err4 = hub.Consumer(cb, "a", "b", "c")

		if err4 != nil {
			t.Fatal(err4)
		}
	}

	<-cbchan1

	for i := 0; i < eventCount; i++ {
		payload := 9 //{A: float64(i), B: `"""string$`, C: []float64{1.0, 2.0, 3.0}}
		//payload := "$$bark$$"
		err7 := hub.Notify("a", []int64{-100000000000, 200000000001, 30}, payload, nil)

		if err7 != nil {
			t.Fatal(err7)
		}
	}

	<-cbchan3

	for i := 0; i < consumerCount; i++ {
		err5 := consumer[i].Subscribe("d", "e", "f", "g")

		if err5 != nil {
			t.Fatal(err5)
		}
	}

	<-cbchan2

	for i := 0; i < eventCount; i++ {
		//payload := Payload1{A: float64(i), B: `"""string$`, C: []float64{1.0, 2.0, 3.0}}
		payload := 9 //"$$bark$$"
		err7 := hub.Notify("b", []int64{-100000000000, 200000000001, 30}, payload, nil)

		if err7 != nil {
			t.Fatal(err7)
		}
	}

	<-cbchan4

	for i := 0; i < consumerCount; i++ {
		err6 := consumer[i].Close()

		if err6 != nil {
			t.Fatal(err6)
		}
	}

	err8 := hub.Stop(true)

	if err8 != nil {
		t.Fatal(err8)
	}

	var stats1 Stats
	var stats2 HubStats
	GetStats(&stats1)
	hub.GetStats(&stats2)

	if stats1.Valid || stats2.Valid {
		logger.Info("stats")
	}
}
