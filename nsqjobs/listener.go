package nsqjobs

import "github.com/nsqio/go-nsq"

type Listener struct{}

func newListener() *Listener {
	return &Listener{}
}

func (l *Listener) HandleMessage(m *nsq.Message) error {
	return nil
}
