package pghub

import (
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
)

type hubCore struct {
	logger  atomic.Pointer[slog.Logger]
	ch      chan *hubMsg
	chDepth atomic.Uint32
	//hubCount	atomic.Uint32
	stats                 stats
	consumerId            atomic.Uint32
	workerId              atomic.Int32
	hubId                 atomic.Uint32
	hubCount              atomic.Uint32
	pool                  msgPool
	mutex                 sync.Mutex
	errStopping           error
	errClosing            error
	errSyntax             error
	supervisoryConnect    Event
	supervisoryDisconnect Event
	supervisoryReconnect  Event
	supervisoryStop       Event
	supervisoryClose      Event
}

func (core *hubCore) lock() {
	core.mutex.Lock()
}

func (core *hubCore) unlock() {
	core.mutex.Unlock()
}

func (core *hubCore) isValidTopic(topic string) bool {
	for _, c := range topic {
		if (c != '_') && !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') {
			return false
		}
	}

	return len(topic) > 0
}

func (core *hubCore) areValidTopics(topics []string) bool {
	if topics != nil {
		for _, topic := range topics {
			if !core.isValidTopic(topic) {
				return false
			}
		}
	}

	return true
}

func init() {
	core.errStopping = errors.New("hub already stopped")
	core.errClosing = errors.New("consumer already closed")
	core.errSyntax = errors.New("decoder syntax error")
	core.ch = make(chan *hubMsg, 1000)

	core.supervisoryConnect = Event{topic: "", key: nil, value: &Advisory{action: ActionConnect, final: false, topic: ""}}
	core.supervisoryDisconnect = Event{topic: "", key: nil, value: &Advisory{action: ActionDisconnect, final: false, topic: ""}}
	core.supervisoryReconnect = Event{topic: "", key: nil, value: &Advisory{action: ActionReconnect, final: false, topic: ""}}
	core.supervisoryStop = Event{topic: "", key: nil, value: &Advisory{action: ActionStop, final: false, topic: ""}}
	core.supervisoryClose = Event{topic: "", key: nil, value: &Advisory{action: ActionClose, final: false, topic: ""}}
}

func (core *hubCore) queueMsg(msg *hubMsg) {
	core.stats.SetQueueMax(core.chDepth.Add(1))
	core.ch <- msg
}

func (core *hubCore) worker() {
	var msg *hubMsg
	var done bool
	//	var keep bool
	var logger *slog.Logger

	id := core.workerId.Add(1)
	core.stats.AddStartCount(1)

	logger = core.logger.Load()

	if logger != nil {
		logger = logger.With(slog.String("source", "core"))
	}

	if logger != nil {
		logger.Info("worker starting (" + strconv.Itoa(int(id)) + ")")
	}

	for !done {
		msg = <-core.ch
		core.chDepth.Add(0xffffffff)

		hub := msg.hub
		msg.hub.fsm(msg)
		core.relMsg(msg)

		if hub.state.Load() == stateStopped {
			count := core.hubCount.Add(0xffffffff)

			if count == 0 {
				done = true
			}
		}
	}

	if logger != nil {
		logger.Info("worker stopping (" + strconv.Itoa(int(id)) + ")")
	}
}

func (core *hubCore) makeHub(name string, config *Config) (*Hub, error) {
	var logger *slog.Logger

	if config.Logger != nil {
		logger = config.Logger
		SetLogger(logger)
	} else {
		logger = core.logger.Load()
	}

	hub := Hub{}

	if len(name) == 0 {
		hub.name = "anonymous"
	} else {
		hub.name = name
	}

	if logger != nil {
		hub.logger = logger.With(slog.String("source", "hub "+name))
	}

	hub.id = core.hubId.Add(1)
	hub.pool = config.Pool
	hub.connectRetry = config.ConnectRetry
	hub.state.Store(stateInit)

	hub.consumers = make(map[uint32]*Consumer)
	hub.topics = make(map[string]map[uint32]*Consumer)

	count := core.hubCount.Add(1)
	core.stats.AddHubCount(1)

	if count == 1 {
		go core.worker()
	}

	hub.mutStop.Lock()

	return &hub, nil
}

func (core *hubCore) getMsg(hub *Hub, code msgCode) *hubMsg {
	msg, avail, alloc := core.pool.get(hub, code)
	core.stats.SetQueueMax(avail)
	core.stats.SetMsgMax(alloc)
	return msg
}

func (core *hubCore) relMsg(msg *hubMsg) {
	core.pool.release(msg)
}

var core = hubCore{}
