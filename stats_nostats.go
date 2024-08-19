//go:build !pghub.stats

package pghub

const includeStats = false

type stats struct{}

func (s *stats) SetMsgMax(delta uint32)     {}
func (s *stats) AddStartCount(delta uint32) {}
func (s *stats) SetQueueMax(max uint32)     {}
func (s *stats) AddHubCount(max uint32)     {}

type hubStats struct{}

func (s *hubStats) AddRxEventCount(delta uint64) {}

func (s *hubStats) AddSubscribeCount(delta uint32)    {}
func (s *hubStats) AddListenCount(delta uint32)       {}
func (s *hubStats) AddListenFailCount(delta uint32)   {}
func (s *hubStats) AddUnlistenCount(delta uint32)     {}
func (s *hubStats) AddUnlistenFailCount(delta uint32) {}
func (s *hubStats) AddNotifyFailCount(delta uint32)   {}
func (s *hubStats) AddNotifyCount(delta uint32)       {}
func (s *hubStats) AddConnectCount(delta uint32)      {}
func (s *hubStats) AddConnectFailCount(delta uint32)  {}
func (s *hubStats) SetQ1Max(max uint32)               {}
func (s *hubStats) SetQ2Max(max uint32)               {}
func (s *hubStats) AddConsumerCount(delta uint32)     {}

func exportStats(out *Stats, in *stats) {
	out.Valid = false
}

func exportHubStats(out *HubStats, in *hubStats) {
	out.Valid = false
}
