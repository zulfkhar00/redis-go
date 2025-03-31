package db

import (
	"sync"
)

type StreamNotification struct {
	mu      sync.Mutex
	waiters map[string][]chan interface{}
}

var StreamNotifier = &StreamNotification{
	waiters: make(map[string][]chan interface{}),
}

func (sn *StreamNotification) RegisterWaiter(streamName string) chan interface{} {
	sn.mu.Lock()
	defer sn.mu.Unlock()

	ch := make(chan interface{}, 1)

	if _, ok := sn.waiters[streamName]; !ok {
		sn.waiters[streamName] = make([]chan interface{}, 0)
	}
	sn.waiters[streamName] = append(sn.waiters[streamName], ch)

	return ch
}

func (sn *StreamNotification) UnRegisterWaiter(streamName string, ch chan interface{}) {
	sn.mu.Lock()
	defer sn.mu.Unlock()

	channels, ok := sn.waiters[streamName]
	if !ok {
		return
	}

	for i, waiter := range channels {
		if waiter == ch {
			sn.waiters[streamName] = append(channels[:i], channels[i+1:]...)
			close(ch)
			return
		}
	}
}

func (sn *StreamNotification) Notify(streamName string, data interface{}) {
	sn.mu.Lock()
	defer sn.mu.Unlock()

	channels, ok := sn.waiters[streamName]
	if !ok {
		return
	}

	for _, ch := range channels {
		ch <- data
	}
}
