package main

import (
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/yosisa/fluxion/message"
	"github.com/yosisa/fluxion/plugin"
	"github.com/yosisa/go-lmq"
)

type Config struct {
	Tag   string
	URL   string
	Queue string
	Multi bool
}

type InLMQ struct {
	env  *plugin.Env
	conf Config
	c    lmq.Client
	pull func(string, time.Duration) (*lmq.Message, error)
	stop chan struct{}
	wg   sync.WaitGroup
}

func (p *InLMQ) Init(env *plugin.Env) (err error) {
	p.env = env
	if err = env.ReadConfig(&p.conf); err != nil {
		return
	}
	p.stop = make(chan struct{})
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
	p.wg.Add(1)
	defer p.wg.Done()

	b := newBackOff()
	fast := make(chan struct{}, 1)
	fast <- struct{}{}
	for {
		select {
		case <-fast:
			b.Reset()
		case <-time.After(b.NextBackOff()):
		case <-p.stop:
			return
		}

		m, err := p.pull(p.conf.Queue, time.Duration(5*time.Second))
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

		select {
		case fast <- struct{}{}:
		default:
		}
	}
}

func (p *InLMQ) Close() error {
	p.env.Log.Info("Waiting for stopping consumer")
	close(p.stop)
	p.wg.Wait()
	p.env.Log.Info("Consumer stopped")
	return nil
}

func newBackOff() backoff.BackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = time.Second
	b.RandomizationFactor = 0.2
	b.Multiplier = 1.2
	b.MaxInterval = 3 * time.Second
	b.MaxElapsedTime = 0
	b.Reset()
	return b
}

func main() {
	plugin.New("in-lmq", func() plugin.Plugin {
		return &InLMQ{}
	}).Run()
}
