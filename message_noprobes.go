//go:build !pghub.probes

package pghub

const includeProbes = false

type hubMsg struct {
	code     msgCode
	hub      *Hub
	consumer *Consumer
	arg1     any
	confirm  ConfirmHook
	next     *hubMsg
}

func (msg *hubMsg) isInPool() bool  { return false }
func (msg *hubMsg) isInQueue() bool { return false }
func (msg *hubMsg) incPool()        {}
func (msg *hubMsg) incQueue()       {}
func (msg *hubMsg) decPool()        {}
func (msg *hubMsg) decQueue()       {}
