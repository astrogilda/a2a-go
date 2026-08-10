// Copyright 2025 The A2A Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eventqueue

import (
	"context"
	"fmt"
	"time"
)

type broadcast struct {
	sender     *inMemoryQueue // sender does not receive its own broadcasts
	payload    *Message
	dispatched chan struct{} // closed after all registered Queues-s received the broadcast.
}

// inMemoryEventBroker manages a goroutine which acts as an event bus for registeted Queue-s.
type inMemoryEventBroker struct {
	registered     map[*inMemoryQueue]any
	destroySignal  chan struct{} // used to request broker destruction
	destroyed      chan struct{} // closed after broker is destroyed
	registerChan   chan *inMemoryQueue
	unregisterChan chan *inMemoryQueue
	broadcastChan  chan *broadcast

	queueBufferSize       int
	slowSubscriberTimeout time.Duration
}

func newInMemoryEventBroker(queueBufferSize int, slowSubscriberTimeout time.Duration) *inMemoryEventBroker {
	broker := &inMemoryEventBroker{
		registered:            make(map[*inMemoryQueue]any),
		registerChan:          make(chan *inMemoryQueue),
		unregisterChan:        make(chan *inMemoryQueue),
		broadcastChan:         make(chan *broadcast),
		destroySignal:         make(chan struct{}),
		destroyed:             make(chan struct{}),
		queueBufferSize:       queueBufferSize,
		slowSubscriberTimeout: slowSubscriberTimeout,
	}
	go func() {
		defer func() {
			for queue := range broker.registered {
				queue.destroy()
			}
			close(broker.destroyed)
		}()

		for {
			select {
			case b := <-broker.broadcastChan:
				if !broker.dispatch(b) {
					return
				}

			case sub := <-broker.registerChan:
				broker.registered[sub] = struct{}{}

			case sub := <-broker.unregisterChan:
				broker.unregister(sub)

			case <-broker.destroySignal:
				return
			}
		}
	}()
	return broker
}

// dispatch fans a broadcast out to every registered queue except the sender and reports
// whether the broker should keep running. The recipients are snapshotted first because
// deliver serves registrations while a send is pending: a queue that registers during a
// broadcast does not receive that broadcast, it starts from the next one.
func (b *inMemoryEventBroker) dispatch(bc *broadcast) bool {
	recipients := make([]*inMemoryQueue, 0, len(b.registered))
	for queue := range b.registered {
		if queue == bc.sender {
			continue
		}
		recipients = append(recipients, queue)
	}
	for _, queue := range recipients {
		if !b.deliver(queue, bc.payload) {
			return false
		}
	}
	close(bc.dispatched)
	return true
}

// deliver hands one payload to one queue and reports whether the broker should keep running.
//
// A send to a full queue is the point where a subscriber that stopped reading used to take
// the whole broker down with it: the broker goroutine parked on the send, so it served no
// registration, no unregistration and no further broadcast, and nothing ever recovered. Two
// things prevent that here. The broker keeps serving registration and unregistration while
// the send is pending, so peers can always connect and disconnect. And a subscriber that
// stays full for slowSubscriberTimeout is dropped, so an abandoned stream degrades only
// itself: its Reader starts returning [ErrQueueClosed] and the client recovers by
// resubscribing, which is cheaper than a gap the client cannot detect.
func (b *inMemoryEventBroker) deliver(queue *inMemoryQueue, payload *Message) bool {
	var slow <-chan time.Time
	if b.slowSubscriberTimeout > 0 {
		timer := time.NewTimer(b.slowSubscriberTimeout)
		defer timer.Stop()
		slow = timer.C
	}
	for {
		if _, registered := b.registered[queue]; !registered {
			// Unregistered while we waited, by its own Close or by the timeout below.
			return true
		}
		select {
		case queue.eventsChan <- payload:
			return true

		case sub := <-b.registerChan:
			b.registered[sub] = struct{}{}

		case sub := <-b.unregisterChan:
			b.unregister(sub)

		case <-slow:
			b.unregister(queue)
			return true

		case <-b.destroySignal:
			return false
		}
	}
}

func (b *inMemoryEventBroker) unregister(queue *inMemoryQueue) {
	if _, ok := b.registered[queue]; !ok {
		return
	}
	delete(b.registered, queue)
	queue.destroy()
}

func (b *inMemoryEventBroker) connect() (*inMemoryQueue, error) {
	conn := &inMemoryQueue{
		broker:     b,
		closedChan: make(chan struct{}),
		eventsChan: make(chan *Message, b.queueBufferSize),
	}
	select {
	case <-b.destroyed:
		return nil, fmt.Errorf("broker destroyed")
	case b.registerChan <- conn:
		return conn, nil
	}
}

func (b *inMemoryEventBroker) destroy() {
	select {
	case <-b.destroyed: // already destroyed
		return
	case b.destroySignal <- struct{}{}: // send destroy signal
	}
	<-b.destroyed
}

// inMemoryQueue implements Queue interface.
type inMemoryQueue struct {
	broker     *inMemoryEventBroker
	closedChan chan struct{}
	eventsChan chan *Message
	closed     bool
}

var _ Reader = (*inMemoryQueue)(nil)
var _ Writer = (*inMemoryQueue)(nil)

func (q *inMemoryQueue) Write(ctx context.Context, message *Message) error {
	if q.closed {
		return ErrQueueClosed
	}

	broadcast := &broadcast{sender: q, payload: message, dispatched: make(chan struct{})}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-q.closedChan:
		return ErrQueueClosed
	case q.broker.broadcastChan <- broadcast:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-q.closedChan:
		return ErrQueueClosed
	case <-broadcast.dispatched:
		// all subscribers received the broadcast
	}
	return nil
}

func (q *inMemoryQueue) Read(ctx context.Context) (*Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case message, ok := <-q.eventsChan: // allow to drain
		if !ok {
			return nil, ErrQueueClosed
		}
		return message, nil
	}
}

func (q *inMemoryQueue) Close() error {
	// loop to ensure we drain q.eventsChan, so that the broker is not blocked trying to send us a broadcast
	for {
		select {
		case _, ok := <-q.eventsChan:
			if !ok {
				return nil
			}
		case q.broker.unregisterChan <- q:
			return nil
		}
	}
}

func (q *inMemoryQueue) destroy() {
	q.closed = true
	close(q.eventsChan)
	close(q.closedChan)
}
