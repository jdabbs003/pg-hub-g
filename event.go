package pghub

type Event struct {
	topic string
	key   []int64
	value any
}

func (event *Event) Topic() string { return event.topic }
func (event *Event) Key() []int64  { return event.key }
func (event *Event) Value() any    { return event.value }

type Action uint

const (
	ActionConnect Action = iota
	ActionDisconnect
	ActionReconnect
	ActionStop
	ActionSubscribe
	ActionClose
)

type Advisory struct {
	action Action
	topic  string
	final  bool
}

func (s *Advisory) Action() Action {
	return s.action
}
func (s *Advisory) Topic() string {
	return s.topic
}
func (s *Advisory) Final() bool {
	return s.final
}
