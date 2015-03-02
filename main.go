package main

import (
	"time"

	"github.com/yosisa/fluxion/buffer"
	"github.com/yosisa/fluxion/message"
	"github.com/yosisa/fluxion/plugin"
	"github.com/yosisa/go-lmq"
)

type Config struct {
	Tag     string
	URL     string
	Queue   string
	Multi   bool
	Timeout buffer.Duration
}

type InLMQ struct {
	env  *plugin.Env
	conf Config
	c    lmq.Client
	pull func(string, time.Duration) (*lmq.Message, error)
}

func (p *InLMQ) Init(env *plugin.Env) (err error) {
	p.env = env
	if err = env.ReadConfig(&p.conf); err != nil {
		return
	}
	if p.conf.Timeout == 0 {
		p.conf.Timeout = buffer.Duration(30 * time.Second)
	}
	return
}

func (p *InLMQ) Start() error {
	p.c = lmq.New(p.conf.URL)
	if !p.conf.Multi {
		p.pull = p.c.Pull
	} else {
		p.pull = p.c.PullAny
	}
	go p.poll()
	return nil
}

func (p *InLMQ) poll() {
	for {
		m, err := p.pull(p.conf.Queue, time.Duration(p.conf.Timeout))
		if err != nil {
			if err2, ok := err.(*lmq.Error); !ok || !err2.IsEmpty() {
				p.env.Log.Error(err)
			}
			continue
		}
		reply := lmq.ReplyAck
		for {
			var v interface{}
			err = m.Decode(&v)
			if err == lmq.EOF {
				break
			} else if err != nil {
				p.env.Log.Error(err)
				reply = lmq.ReplyNack
				continue
			}
			vv, ok := v.(map[string]interface{})
			if !ok {
				vv = map[string]interface{}{"message": v}
			}
			p.env.Emit(message.NewEvent(p.conf.Tag, vv))
		}
		if err = p.c.Reply(m, reply); err != nil {
			p.env.Log.Error(err)
		}
	}
}

func (p *InLMQ) Close() error {
	return nil
}

func main() {
	plugin.New("in-lmq", func() plugin.Plugin {
		return &InLMQ{}
	}).Run()
}
