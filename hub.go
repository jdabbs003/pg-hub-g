package pghub

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	stateInit uint32 = iota
	stateConnecting
	stateWaiting
	stateListening
	stateSwitching
	stateTalking
	stateStopping
	stateStopped
)

const (
	hmcTaskSuccess   msgCode = 0x0000
	hmcTaskFail              = 0x0001
	hmcTaskCancelled         = 0x0002

	hmcMaskCode    = 0xff00
	hmcMaskSubcode = 0x00ff
)

const (
	hmcNull          = 0x0000
	hmcUser          = 0x0100
	hmcUserConsumer  = hmcUser + 0x0001
	hmcUserSubscribe = hmcUser + 0x0002
	hmcUserClose     = hmcUser + 0x0003
	hmcUserNotify    = hmcUser + 0x0004

	hmcStart      = 0x0200
	hmcStop       = 0x0300
	hmcRecvNotify = 0x0400

	hmcTaskListen        = 0x0500
	hmcTaskListenCancel  = hmcTaskListen + hmcTaskCancelled
	hmcTaskListenFailure = hmcTaskListen + hmcTaskFail

	hmcTaskTimer          = 0x0600
	hmcTaskTimerCancelled = hmcTaskTimer + hmcTaskCancelled
	hmcTaskTimerSuccess   = hmcTaskTimer + hmcTaskSuccess

	hmcTaskConnect        = 0x0700
	hmcTaskConnectSuccess = hmcTaskConnect + hmcTaskSuccess
	hmcTaskConnectFail    = hmcTaskConnect + hmcTaskFail

	hmcTaskQ1        = 0x0800
	hmcTaskQ1Success = hmcTaskQ1 + hmcTaskSuccess
	hmcTaskQ1Cancel  = hmcTaskQ1 + hmcTaskCancelled
	hmcTaskQ1Failure = hmcTaskQ1 + hmcTaskFail

	hmcTaskQ2        = 0x0900
	hmcTaskQ2Success = hmcTaskQ2 + hmcTaskSuccess
	hmcTaskQ2Cancel  = hmcTaskQ2 + hmcTaskCancelled
	hmcTaskQ2Failure = hmcTaskQ2 + hmcTaskFail
)

type listenCommand struct {
	listen bool
	topic  string
}

type Hub struct {
	id           uint32
	name         string
	taskQ1       task
	taskQ2       task
	taskTimer    task
	taskConnect  task
	taskListen   task
	con          atomic.Pointer[pgxpool.Conn]
	connectRetry time.Duration
	state        atomic.Uint32
	pool         *pgxpool.Pool
	logger       *slog.Logger
	consumers    map[uint32]*Consumer
	topics       map[string]map[uint32]*Consumer
	obQ1         msgQueue
	obQ2         msgQueue
	timer        *time.Timer
	stopReq      bool
	mutStop      sync.Mutex
	working      bool
	hasConnected bool
	stats        hubStats
}

type task struct {
	running atomic.Bool
	cancel  func()
}

func (t *task) start() bool {
	return t.running.CompareAndSwap(false, true)
}

func (hub *Hub) stateString(state uint32) string {
	switch state {
	case stateInit:
		return "init"
	case stateConnecting:
		return "connecting"
	case stateWaiting:
		return "waiting"
	case stateListening:
		return "listening"
	case stateSwitching:
		return "switching"
	case stateTalking:
		return "talking"
	case stateStopping:
		return "stopping"
	case stateStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

func (hub *Hub) queueMsg(msg *hubMsg, consumer *Consumer) error {
	core.lock()
	defer core.unlock()

	if (consumer != nil) && (consumer.closeReq) {
		core.relMsg(msg)

		if msg.code == hmcUserClose {
			return nil
		} else {
			return core.errClosing
		}
	}

	if hub.stopReq {
		core.relMsg(msg)

		if msg.code == hmcStop {
			return nil
		} else {
			return core.errStopping
		}
	}

	if msg.code == hmcUserClose {
		consumer.closeReq = true
	} else if msg.code == hmcStop {
		hub.stopReq = true
	}

	core.queueMsg(msg)
	return nil
}

func (hub *Hub) Start() error {
	msg := core.getMsg(hub, hmcStart)
	return hub.queueMsg(msg, nil)
}

func (hub *Hub) Stop(wait bool) error {
	msg := core.getMsg(hub, hmcStop)
	err := hub.queueMsg(msg, nil)

	if (err == nil) && wait {
		hub.mutStop.Lock()
		hub.mutStop.Unlock()
	}

	return err
}

func (hub *Hub) Notify(topic string, key []int64, value any, confirm ConfirmHook) error {
	s, err := EncodeEvent(topic, key, value)

	if err != nil {
		return err
	}

	msg := core.getMsg(hub, hmcUserNotify)
	msg.arg1 = s
	msg.confirm = confirm

	return hub.queueMsg(msg, nil)
}

func (hub *Hub) exeNotify(msg *hubMsg) {
	msg2 := core.getMsg(hub, hmcNull)
	msg2.arg1 = msg.arg1
	qcount := hub.obQ2.put(msg2)
	hub.stats.SetQ2Max(qcount)
	hub.startTaskQ2()
}

func (hub *Hub) exeSubscribe(msg *hubMsg) {
	c := msg.consumer
	topics := msg.arg1.([]string) //topics
	commands := make([]listenCommand, len(topics))
	var commandCount = 0
	var subscribeCount uint32 = 0

	for _, topic := range topics {
		if c.topics[topic] == 0 {
			c.topics[topic] = 1
			subscribeCount += 1
			topicSet := hub.topics[topic]

			if topicSet == nil {
				topicSet = make(map[uint32]*Consumer)
				hub.topics[topic] = topicSet
				commands[commandCount] = listenCommand{true, topic}
				commandCount++
			}

			topicSet[c.id] = c
			c.notify(&Event{topic: "", key: nil, value: &Advisory{action: ActionSubscribe, final: false, topic: topic}}, false)
		}
	}

	hub.stats.AddSubscribeCount(subscribeCount)

	if commandCount != 0 {
		msg2 := core.getMsg(hub, hmcNull)
		msg2.arg1 = commands[:commandCount]
		qcount := hub.obQ1.put(msg2)
		hub.stats.SetQ1Max(qcount)
	}
}

func (hub *Hub) subscribe(consumer *Consumer, topics []string) error {
	for _, topic := range topics {
		if !core.isValidTopic(topic) {
			return errors.New("invalid topic")
		}
	}

	msg := core.getMsg(hub, hmcUserSubscribe)
	msg.consumer = consumer
	msg.arg1 = topics

	return hub.queueMsg(msg, consumer)
}

func (hub *Hub) exeClose(msg *hubMsg) {
	c := msg.consumer
	topics := c.topics
	commands := make([]listenCommand, len(topics))
	commandCount := 0

	for topic, _ := range topics {
		topicSet := hub.topics[topic]
		delete(topicSet, c.id)

		if len(topicSet) == 0 {
			delete(hub.topics, topic)
			commands[commandCount] = listenCommand{false, topic}
			commandCount++
		}
	}

	delete(hub.consumers, c.id)
	c.notify(&core.supervisoryClose, true)

	if commandCount != 0 {
		msg2 := core.getMsg(hub, hmcNull)
		msg2.arg1 = commands[:commandCount]
		qcount := hub.obQ1.put(msg2)
		hub.stats.SetQ1Max(qcount)
	}
}

func (hub *Hub) close(consumer *Consumer) error {
	msg := core.getMsg(hub, hmcUserClose)
	msg.consumer = consumer

	return hub.queueMsg(msg, consumer)
}

func (hub *Hub) GetStats(stats *HubStats) {
	exportHubStats(stats, &hub.stats)
}

func (hub *Hub) Consumer(notify NotifyHook, topics ...string) (*Consumer, error) {
	if (topics != nil) && !core.areValidTopics(topics) {
		return nil, errors.New("invalid topic")
	}

	id := core.consumerId.Add(1)

	consumer := &Consumer{hub: hub, id: id, hook: notify, topics: make(map[string]uint)}

	msg := core.getMsg(hub, hmcUserConsumer)
	msg.consumer = consumer
	msg.arg1 = topics

	err := hub.queueMsg(msg, consumer)

	if err != nil {
		return nil, err
	} else {
		hub.stats.AddConsumerCount(1)
		return consumer, nil
	}
}

func (hub *Hub) exeConsumer(msg *hubMsg) {
	c := msg.consumer
	topics := msg.arg1.([]string)

	hub.consumers[c.id] = c

	if topics != nil {
		hub.exeSubscribe(msg)
	}
}

func (hub *Hub) startTaskQ1() bool {
	t := &hub.taskQ1

	if t.start() {
		var cancel atomic.Bool
		t.cancel = func() { cancel.Store(true) }

		go func() {
			var msg *hubMsg
			con := hub.con.Load()
			var builder strings.Builder

			hub.logDebug("starting task Q1")

			for {
				if cancel.Load() {
					hub.logDebug("stopping task Q1 (cancel)")
					msg = core.getMsg(hub, hmcTaskQ1Cancel)
					t.running.Store(false)
					core.queueMsg(msg)
					return
				}

				msg := hub.obQ1.get()

				if msg == nil {
					hub.logDebug("stopping task Q1 (complete)")
					msg = core.getMsg(hub, hmcTaskQ1Success)
					t.running.Store(false)
					core.queueMsg(msg)
					return
				}

				commands := msg.arg1.([]listenCommand)

				if commands != nil {
					for _, command := range commands {
						builder.Reset()
						if command.listen {
							builder.WriteString("listen ")
							hub.stats.AddListenCount(1)
						} else {
							builder.WriteString("unlisten ")
							hub.stats.AddUnlistenCount(1)
						}

						builder.WriteString(command.topic)
						commandString := builder.String()

						_, err := con.Exec(context.Background(), commandString)

						if err != nil {
							if command.listen {
								hub.stats.AddListenFailCount(1)
							} else {
								hub.stats.AddUnlistenFailCount(1)
							}

							hub.logDebug("stopping task Q1 (fail)")
							msg.code = hmcTaskQ1Failure
							msg.arg1 = err
							t.running.Store(false)
							core.queueMsg(msg)
							return
						}
					}
				}

				core.relMsg(msg)
			}
		}()

		return true
	} else {
		return false
	}
}

func (hub *Hub) startTaskQ2() bool {
	if hub.obQ2.peek() == nil {
		return false
	}

	state := hub.state.Load()

	if (state != stateTalking) && (state != stateListening) && (state != stateSwitching) {
		return false
	}

	t := &hub.taskQ2

	if t.start() {
		var cancel atomic.Bool
		t.cancel = func() { cancel.Store(true) }

		hub.logDebug("starting task Q2")

		go func() {
			var msg *hubMsg

			for {
				state := hub.state.Load()
				if (state != stateTalking) && (state != stateListening) && (state != stateSwitching) {
					msg = core.getMsg(hub, hmcTaskQ2Cancel)
					hub.logDebug("stopping task Q2 (state)")
					t.running.Store(false)
					core.queueMsg(msg)
					return
				} else {
					msg := hub.obQ2.peek()

					if msg == nil {
						msg = core.getMsg(hub, hmcTaskQ2Success)
						hub.logDebug("stopping task Q2 (complete)")
						t.running.Store(false)
						core.queueMsg(msg)
						return
					} else {
						command := msg.arg1.(string)
						_, err := hub.pool.Exec(context.Background(), command)
						hub.stats.AddNotifyCount(1)

						if err != nil {
							hub.stats.AddNotifyFailCount(1)
							hub.logWarn("tx event to pg (failed)")
							hub.logDebug("stopping task Q2 (failed)")
							msg = core.getMsg(hub, hmcTaskQ2Failure)
							msg.arg1 = err
							t.running.Store(false)
							core.queueMsg(msg)
							return
						} else {
							hub.logDebug("tx event to pg")
							if msg.confirm != nil {
								msg.confirm(true)
							}

							msg2 := hub.obQ2.get()

							if includeProbes && (msg2 != msg) {
								panic("probe 1")
							}

							core.relMsg(msg)
						}
					}
				}
			}
		}()

		return true
	}

	return false
}

func (hub *Hub) startTaskTimer(duration time.Duration) {
	t := &hub.taskTimer

	if t.start() {
		timer := time.NewTimer(duration)
		t.cancel = func() {
			timer.Stop()
		}

		hub.logDebug("starting task T")

		go func() {
			msg := core.getMsg(hub, hmcTaskTimerCancelled)
			for range timer.C {
				msg.code = hmcTaskTimerSuccess
			}

			if msg.code == hmcTaskTimerSuccess {
				hub.logDebug("stopping task T (expire)")
			} else {
				hub.logDebug("stopping task T (cancel)")
			}

			t.running.Store(false)
			core.queueMsg(msg)
		}()
	} else {
		panic("task t already started")
	}
}

func (hub *Hub) startTaskConnect() {
	t := &hub.taskConnect

	if t.start() {
		hub.logDebug("starting task C")

		go func() {
			con, err := hub.pool.Acquire(context.Background())

			msg := core.getMsg(hub, hmcTaskConnectSuccess)

			if err != nil {
				msg.code = hmcTaskConnectFail
				hub.logDebug("stopping task C (fail)")
				msg.arg1 = err
			} else {
				hub.logDebug("stopping task C (complete)")
				msg.arg1 = con
			}

			t.running.Store(false)
			core.queueMsg(msg)
		}()
	} else {
		panic("task c already started")
	}
}

func (hub *Hub) startTaskListen() {
	t := &hub.taskListen

	if t.start() {
		con := hub.con.Load()
		var ctx context.Context
		ctx, t.cancel = context.WithCancel(context.Background())

		hub.logDebug("starting task L")

		go func() {
			for {
				n, err1 := con.Conn().WaitForNotification(ctx)

				if err1 == nil {
					event, err2 := DecodeEvent(n)

					if err2 == nil {
						msg := core.getMsg(hub, hmcRecvNotify)
						msg.arg1 = event
						hub.stats.AddRxEventCount(1)
						core.queueMsg(msg)
						hub.logDebug("rx event from pg (chan " + n.Channel + ")")
					} else {
						hub.logWarn("rx invalid event from pg (chan " + n.Channel + ")")
					}
				} else {
					var msg *hubMsg
					if errors.Is(err1, context.Canceled) {
						msg = core.getMsg(hub, hmcTaskListenCancel)
						hub.logDebug("stopping task L (cancel)")
					} else {
						msg = core.getMsg(hub, hmcTaskListenFailure)
						hub.logDebug("stopping task L (fail)")
						msg.arg1 = err1
					}

					t.running.Store(false)
					core.queueMsg(msg)

					return
				}
			}
		}()
	} else {
		panic("task l already started")
	}
}

func (hub *Hub) logDebug(msg string, attrs ...slog.Attr) {
	if hub.logger != nil {
		hub.logger.LogAttrs(context.Background(), slog.LevelDebug, msg, attrs...)
	}
}

func (hub *Hub) logInfo(msg string, attrs ...slog.Attr) {
	if hub.logger != nil {
		hub.logger.LogAttrs(context.Background(), slog.LevelInfo, msg, attrs...)
	}
}

func (hub *Hub) logWarn(msg string, attrs ...slog.Attr) {
	if hub.logger != nil {
		hub.logger.LogAttrs(context.Background(), slog.LevelWarn, msg, attrs...)
	}
}

func (hub *Hub) logError(msg string, attrs ...slog.Attr) {
	if hub.logger != nil {
		hub.logger.LogAttrs(context.Background(), slog.LevelError, msg, attrs...)
	}
}

func (hub *Hub) changeState(newState uint32) {
	oldState := hub.state.Load()

	if newState != oldState {
		hub.logInfo("state " + hub.stateString(oldState) + " -> " + hub.stateString(newState))
		hub.state.Store(newState)

		if newState == stateStopped {
			hub.mutStop.Unlock()
		}

		hub.startTaskQ2()
	}
}

func (hub *Hub) fsmHandleQ2(msg *hubMsg) {
	switch hub.state.Load() {
	case stateConnecting:
	case stateWaiting:
		{
		}

	case stateSwitching:
	case stateTalking:
	case stateListening:
		{
			hub.startTaskQ2()
		}

	case stateStopping:
		{
			hub.fsmHandleStopping(msg)
		}

	default:
		{
			panic("bad message (1)")
		}
	}
}

func (hub *Hub) fsmHandleStop(msg *hubMsg) {
	needStopping := false

	hub.broadcast(&core.supervisoryStop, true)

	hub.consumers = nil
	hub.topics = nil

	hub.exeDisposeConnection()

	if hub.taskConnect.running.Load() {
		needStopping = true
	}

	if hub.taskQ2.running.Load() {
		needStopping = true
	}

	if hub.taskQ1.running.Load() {
		needStopping = true
		hub.taskQ1.cancel()
	}

	if hub.taskListen.running.Load() {
		needStopping = true
		hub.taskListen.cancel()
	}

	if hub.taskTimer.running.Load() {
		needStopping = true
		hub.taskTimer.cancel()
	}

	if needStopping {
		hub.changeState(stateStopping)
	} else {
		hub.changeState(stateStopped)
	}
}

func (hub *Hub) broadcast(event *Event, final bool) {
	for _, consumer := range hub.consumers {
		consumer.notify(event, final)
	}
}

func (hub *Hub) broadcastTopic(event *Event) {
	topic := hub.topics[event.topic]

	if topic == nil {
		hub.logWarn("spurious topic '" + event.topic + "'")
	} else {
		for _, consumer := range topic {
			consumer.notify(event, false)
		}
	}
}

func (hub *Hub) exeDisposeConnection() {
	con := hub.con.Load()

	if con != nil {
		hub.con.Store(nil)

		go func() {
			_, _ = con.Exec(context.Background(), "unlisten *")
			hub.stats.AddUnlistenCount(1)
			con.Release()
		}()
	}
}

func (hub *Hub) fsmHandleUserInput(msg *hubMsg) {
	switch msg.code {
	case hmcUserConsumer:
		hub.exeConsumer(msg)
	case hmcUserSubscribe:
		hub.exeSubscribe(msg)
	case hmcUserClose:
		hub.exeClose(msg)
	case hmcUserNotify:
		hub.exeNotify(msg)
	default:
		{
			panic("bad message (2)")
		}
	}
}

func (hub *Hub) fsmHandleStopping(msg *hubMsg) {
	ready := true
	if hub.taskConnect.running.Load() {
		ready = false
	}

	if hub.taskQ2.running.Load() {
		ready = false
	}

	if hub.taskQ1.running.Load() {
		ready = false
	}

	if hub.taskListen.running.Load() {
		ready = false
	}

	if hub.taskTimer.running.Load() {
		ready = false
	}

	if ready {
		hub.obQ2.clear()
		hub.changeState(stateStopped)
	}
}

var asterisk = "*"

func (hub *Hub) fsm(msg *hubMsg) {
	code := msg.code & hmcMaskCode
	subcode := msg.code & hmcMaskSubcode

	if code == hmcStop {
		hub.fsmHandleStop(msg)
	} else if code == hmcTaskQ2 {
		hub.fsmHandleQ2(msg)
	} else {
		switch hub.state.Load() {
		case stateInit:
			{
				switch code {
				case hmcStart:
					{
						hub.startTaskConnect()
						hub.changeState(stateConnecting)
					}

				case hmcUser:
					{
						hub.fsmHandleUserInput(msg)
					}

				default:
					{
						panic("bad message (3)")
					}
				}
			}

		case stateConnecting:
			{
				switch code {
				case hmcStart:
					{
						hub.logDebug(`spurious start`)
					}

				case hmcTaskConnectSuccess:
					{
						hub.stats.AddConnectCount(1)
						hub.obQ1.clear()
						hub.con.Store(msg.arg1.(*pgxpool.Conn))

						commands := make([]listenCommand, len(hub.topics)+1)
						commandCount := uint32(0)

						commands[commandCount] = listenCommand{false, asterisk}
						commandCount++

						for topic, _ := range hub.topics {
							commands[commandCount] = listenCommand{true, topic}
							commandCount++
						}

						hub.stats.AddSubscribeCount(commandCount)

						msg2 := core.getMsg(hub, hmcNull)
						msg2.arg1 = commands
						qcount := hub.obQ1.put(msg2)
						hub.stats.SetQ1Max(qcount)

						if hub.hasConnected {
							hub.broadcast(&core.supervisoryReconnect, false)
						} else {
							hub.broadcast(&core.supervisoryConnect, false)
							hub.hasConnected = true
						}

						if hub.obQ1.peek() != nil {
							hub.startTaskQ1()
							hub.changeState(stateTalking)
						} else {
							hub.changeState(stateListening)
						}

						hub.startTaskQ2()
					}

				case hmcTaskConnectFail:
					{
						hub.logInfo("connect fail")
						hub.startTaskTimer(hub.connectRetry)
						hub.changeState(stateWaiting)
					}

				case hmcUser:
					{
						hub.fsmHandleUserInput(msg)
					}

				default:
					{
						panic("bad message (4)")
					}
				}
			}

		case stateWaiting:
			{
				switch code {
				case hmcStart:
					{
						hub.logDebug("spurious start")
					}

				case hmcTaskTimerSuccess:
					{
						hub.startTaskConnect()
						hub.changeState(stateConnecting)
					}

				case hmcUser:
					{
						hub.fsmHandleUserInput(msg)
					}

				default:
					{
						panic("bad message (5)")
					}
				}
			}

		case stateTalking:
			{
				switch code {
				case hmcStart:
					{
						hub.logDebug("spurious start")
					}

				case hmcTaskQ1:
					{
						switch subcode {
						case hmcTaskSuccess:
							{
								if hub.obQ1.peek() != nil {
									hub.startTaskQ1()
								} else {
									hub.startTaskListen()
									hub.changeState(stateListening)
								}
							}

						case hmcTaskFail:
							{
								hub.exeDisposeConnection()
								hub.startTaskConnect()

								hub.broadcast(&core.supervisoryDisconnect, false)

								hub.changeState(stateConnecting)
							}

						default:
							{
								panic("bad message (6)")
							}
						}
					}

				case hmcUser:
					{
						hub.fsmHandleUserInput(msg)
					}

				default:
					{
						panic("bad message (7)")
					}
				}
			}

		case stateListening:
			{
				switch code {
				case hmcStart:
					{
						hub.logDebug("spurious start")
					}

				case hmcRecvNotify:
					{
						hub.broadcastTopic(msg.arg1.(*Event))
					}

				case hmcTaskListen:
					{
						if subcode == hmcTaskFail {
							hub.exeDisposeConnection()
							hub.startTaskConnect()

							hub.broadcast(&core.supervisoryDisconnect, false)

							hub.changeState(stateConnecting)
						} else {
							panic("bad message (8)")
						}
					}

				case hmcUser:
					{
						hub.fsmHandleUserInput(msg)

						if hub.obQ1.peek() != nil {
							hub.taskListen.cancel()
							hub.changeState(stateSwitching)
						}
					}

				default:
					{
						panic("bad message (9)")
					}
				}
			}

		case stateSwitching:
			{
				switch code {
				case hmcStart:
					{
						hub.logDebug("spurious start")
					}

				case hmcRecvNotify:
					{
						hub.broadcastTopic(msg.arg1.(*Event))
					}

				case hmcUser:
					{
						hub.fsmHandleUserInput(msg)
					}

				case hmcTaskListen:
					{
						if subcode == hmcTaskCancelled {
							hub.changeState(stateTalking)
							hub.startTaskQ1()
						} else if subcode == hmcTaskFail {
							hub.exeDisposeConnection()
							hub.startTaskConnect()

							hub.broadcast(&core.supervisoryDisconnect, false)

							hub.changeState(stateConnecting)
						} else {
							panic("bad message (10)")
						}
					}

				default:
					{
						panic("bad message (11)")
					}
				}
			}

		case stateStopping:
			{
				hub.fsmHandleStopping(msg)
			}

		case stateStopped:
			{
			}

		default:
			{
				panic("bad message (12)")
			}
		}
	}
}
