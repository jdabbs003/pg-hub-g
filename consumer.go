package pghub

import "errors"

type Consumer struct {
	id       uint32
	closeReq bool //atomic.Bool
	hub      *Hub
	hook     NotifyHook
	topics   map[string]uint
}

func (c *Consumer) Close() error {
	if c.hub != nil {
		return c.hub.close(c)
	} else {
		return nil
	}
}

func (c *Consumer) Subscribe(topics ...string) error {
	for _, topic := range topics {
		if !core.areValidTopics(topics) {
			return errors.New("invalid topic: " + topic)
		}
	}
	if c.hub != nil {
		return c.hub.subscribe(c, topics)
	} else {
		return nil
	}
}

//func (c *Consumer) dispose() {
//	c.hub = nil
//	c.topics = nil
//	c.hook = nil
//}

func (c *Consumer) notify(event *Event, final bool) {
	hook := c.hook

	if final {
		c.hub = nil
		c.topics = nil
		c.hook = nil
	}

	if hook != nil {
		hook(event)
	}
}
