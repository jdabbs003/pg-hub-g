//go:build pghub.stats

package pghub

import "sync/atomic"

const includeStats = true

func setMax64(value *atomic.Uint64, newValue uint64) bool {
	for {
		oldValue := value.Load()

		if newValue > oldValue {
			if value.CompareAndSwap(oldValue, newValue) {
				return true
			}
		} else {
			return false
		}
	}
}

func setMax32(value *atomic.Uint32, newValue uint32) bool {
	for {
		oldValue := value.Load()

		if newValue > oldValue {
			if value.CompareAndSwap(oldValue, newValue) {
				return true
			}
		} else {
			return false
		}
	}
}

type stats struct {
	msgMax     atomic.Uint32
	startCount atomic.Uint32
	queueMax   atomic.Uint32
	hubCount   atomic.Uint32
}

func (s *stats) SetMsgMax(count uint32) {
	setMax32(&s.msgMax, count)
}

func (s *stats) AddStartCount(delta uint32) {
	s.startCount.Add(delta)
}

func (s *stats) SetQueueMax(max uint32) {
	setMax32(&s.queueMax, max)
}

func (s *stats) SetAllocMax(max uint32) {
	setMax32(&s.queueMax, max)
}

func (s *stats) AddHubCount(max uint32) {
	s.hubCount.Add(1)
}

type hubStats struct {
	rxEventCount      atomic.Uint64
	subscribeCount    atomic.Uint32
	listenCount       atomic.Uint32
	listenFailCount   atomic.Uint32
	unlistenCount     atomic.Uint32
	unlistenFailCount atomic.Uint32
	notifyCount       atomic.Uint32
	notifyFailCount   atomic.Uint32
	connectCount      atomic.Uint64
	connectFailCount  atomic.Uint64
	q1Max             atomic.Uint32
	q2Max             atomic.Uint32
	consumerCount     atomic.Uint32
}

func (s *hubStats) AddRxEventCount(delta uint64) {
	s.rxEventCount.Add(delta)
}

func (s *hubStats) AddSubscribeCount(delta uint32) {
	s.subscribeCount.Add(delta)
}

func (s *hubStats) AddListenCount(delta uint32) {
	s.listenCount.Add(delta)
}

func (s *hubStats) AddListenFailCount(delta uint32) {
	s.listenFailCount.Add(delta)
}

func (s *hubStats) AddUnlistenCount(delta uint32) {
	s.unlistenCount.Add(delta)
}

func (s *hubStats) AddUnlistenFailCount(delta uint32) {
	s.unlistenFailCount.Add(delta)
}

func (s *hubStats) AddNotifyCount(delta uint32) {
	s.notifyCount.Add(delta)
}

func (s *hubStats) AddNotifyFailCount(delta uint32) {
	s.notifyCount.Add(delta)
}

func (s *hubStats) AddConnectCount(delta uint64) {
	s.connectCount.Add(delta)
}

func (s *hubStats) AddConnectFailCount(delta uint64) {
	s.connectFailCount.Add(delta)
}

func (s *hubStats) SetQ1Max(max uint32) {
	setMax32(&s.q1Max, max)
}

func (s *hubStats) SetQ2Max(max uint32) {
	setMax32(&s.q2Max, max)
}

func (s *hubStats) AddConsumerCount(delta uint32) {
	s.consumerCount.Add(delta)
}

func exportStats(out *Stats, in *stats) {
	out.Valid = true
	out.MsgMax = in.msgMax.Load()
	out.StartCount = in.startCount.Load()
	out.QueueMax = in.queueMax.Load()
	out.HubCount = in.hubCount.Load()
}

func exportHubStats(out *HubStats, in *hubStats) {
	out.Valid = true
	out.RxEventCount = in.rxEventCount.Load()
	out.SubscribeCount = in.subscribeCount.Load()
	out.ListenCount = in.listenCount.Load()
	out.ListenFailCount = in.listenFailCount.Load()
	out.UnlistenCount = in.unlistenCount.Load()
	out.UnlistenFailCount = in.unlistenFailCount.Load()
	out.NotifyCount = in.notifyCount.Load()
	out.NotifyFailCount = in.notifyFailCount.Load()
	out.ConnectCount = in.connectCount.Load()
	out.ConnectFailCount = in.connectFailCount.Load()
	out.Q1Max = in.q1Max.Load()
	out.Q2Max = in.q2Max.Load()
	out.ConsumerCount = in.consumerCount.Load()
}
