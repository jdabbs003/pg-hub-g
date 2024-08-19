package pghub

type msgCode uint16

type msgQueue struct {
	head  *hubMsg
	tail  *hubMsg
	count uint32
}

type msgPool struct {
	head  *hubMsg
	avail uint32
	alloc uint32
}

func (q *msgQueue) get() *hubMsg {
	core.lock()
	defer core.unlock()

	msg := q.head

	if msg != nil {
		q.head = msg.next

		if q.head == nil {
			q.tail = nil
		}

		q.count--
		msg.decQueue()
	}

	if includeProbes && (msg != nil) && (msg.isInPool() || msg.isInQueue()) {
		panic("message queue lifecycle 1a")
	}

	return msg
}

func (q *msgQueue) peek() *hubMsg {
	core.lock()
	defer core.unlock()

	msg := q.head

	if includeProbes && (msg != nil) && (msg.isInPool() || !msg.isInQueue()) {
		panic("message queue lifecycle 1b")
	}

	return msg
}

func (q *msgQueue) put(msg *hubMsg) uint32 {
	core.lock()
	defer core.unlock()

	if includeProbes && (msg.isInPool() || msg.isInQueue()) {
		panic("message queue lifecycle 1")
	}

	if q.tail != nil {
		q.tail.next = msg
	} else {
		q.head = msg
	}

	q.count++
	q.tail = msg
	msg.next = nil
	msg.incQueue()
	return q.count
}

func (q *msgQueue) clear() {
	msg := q.get()

	for msg != nil {
		if msg.confirm != nil {
			msg.confirm(false)
		}

		core.relMsg(msg)
		msg = q.get()
	}
}

func (pool *msgPool) get(hub *Hub, code msgCode) (msg *hubMsg, avail uint32, alloc uint32) {
	core.lock()
	defer core.unlock()

	const increment uint32 = 32

	if pool.head == nil {
		msgs := [increment]hubMsg{}

		for i, _ := range msgs {
			msgs[i].incPool()

			if i < (len(msgs) - 1) {
				msgs[i].next = &msgs[i+1]
			}
		}

		//		msg = &msgs[len(msgs)-1]
		pool.head = &msgs[0]

		pool.avail += increment
		pool.alloc += increment
	}

	msg = pool.head
	pool.head = pool.head.next
	pool.avail -= 1
	msg.decPool()

	if includeProbes && (msg.isInPool() || msg.isInQueue()) {
		panic("message pool lifecycle 1")
	}

	msg.code = code
	msg.hub = hub
	return msg, pool.avail, pool.alloc
}

func (pool *msgPool) release(msg *hubMsg) {
	msg.hub = nil
	msg.consumer = nil
	msg.arg1 = nil
	msg.confirm = nil

	core.lock()
	defer core.unlock()

	if includeProbes && (msg.isInPool() || msg.isInQueue()) {
		panic("message pool lifecycle 2")
	}

	msg.next = pool.head
	pool.head = msg
	pool.avail += 1
	msg.incPool()
}
