//go:build pghub.probes

package pghub

const includeProbes = true

type hubMsg struct {
	code     msgCode
	hub      *Hub
	consumer *Consumer
	arg1     any
	confirm  ConfirmHook
	next     *hubMsg
	inPool   uint32
	inQueue  uint32
}

func (msg *hubMsg) isInPool() bool  { return msg.inPool != 0 }
func (msg *hubMsg) isInQueue() bool { return msg.inQueue != 0 }
func (msg *hubMsg) incPool()        { msg.inPool++ }
func (msg *hubMsg) incQueue()       { msg.inQueue++ }
func (msg *hubMsg) decPool()        { msg.inPool-- }
func (msg *hubMsg) decQueue()       { msg.inQueue-- }
