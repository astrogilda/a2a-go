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
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"
)

// assertionTimeout is how long a test waits for an operation that must not be held up by a
// subscriber that stopped reading, before declaring the broker wedged.
const assertionTimeout = 3 * time.Second

type eventVersionPair struct {
	event   a2a.Event
	version taskstore.TaskVersion
}

func newUnversioned(event a2a.Event) *eventVersionPair {
	return &eventVersionPair{event: event, version: taskstore.TaskVersionMissing}
}

func mustCreateReadWriter(t *testing.T, qm Manager, tid a2a.TaskID) (Reader, Writer) {
	t.Helper()
	r, err := qm.CreateReader(t.Context(), tid)
	if err != nil {
		t.Fatalf("qm.CreateReader() error = %v", err)
	}
	w, err := qm.CreateWriter(t.Context(), tid)
	if err != nil {
		t.Fatalf("qm.CreateWriter() error = %v", err)
	}
	return r, w
}

func mustWrite(t *testing.T, q Writer, messages ...*eventVersionPair) {
	t.Helper()
	for i, msg := range messages {
		if err := q.Write(t.Context(), &Message{Event: msg.event, TaskVersion: msg.version}); err != nil {
			t.Fatalf("q.Write() error = %v at %d", err, i)
		}
	}
}

func mustRead(t *testing.T, q Reader) (a2a.Event, taskstore.TaskVersion) {
	t.Helper()
	result, err := q.Read(t.Context())
	if err != nil {
		t.Fatalf("q.Read() error = %v", err)
	}
	return result.Event, result.TaskVersion
}

func newTestManager(t *testing.T, opts ...MemManagerOption) Manager {
	qm := NewInMemoryManager(opts...)
	t.Cleanup(func() {
		manager := qm.(*inMemoryManager)
		var ids []a2a.TaskID
		for tid := range manager.brokers {
			ids = append(ids, tid)
		}
		for _, tid := range ids {
			if err := qm.Destroy(t.Context(), tid); err != nil {
				t.Fatalf("qm.Destroy() error = %v", err)
			}
		}
	})
	return qm
}

func TestInMemoryQueue_WriteRead(t *testing.T) {
	t.Parallel()
	qm := newTestManager(t)

	tid := a2a.NewTaskID()
	readQueue, writeQueue := mustCreateReadWriter(t, qm, tid)

	want := &eventVersionPair{event: &a2a.Message{ID: "test-event"}, version: taskstore.TaskVersion(1)}
	mustWrite(t, writeQueue, want)
	got, gotVersion := mustRead(t, readQueue)
	if !reflect.DeepEqual(got, want.event) {
		t.Errorf("Read() got = %v, want %v", got, want)
	}
	if gotVersion != taskstore.TaskVersion(1) {
		t.Errorf("Read() got version = %v, want %v", gotVersion, taskstore.TaskVersion(1))
	}
}

func TestInMemoryQueue_DrainAfterDestroy(t *testing.T) {
	t.Parallel()
	qm := newTestManager(t)
	ctx, tid := t.Context(), a2a.NewTaskID()

	readQueue, writeQueue := mustCreateReadWriter(t, qm, tid)
	want := []*eventVersionPair{
		{event: &a2a.Message{ID: "test-event"}, version: taskstore.TaskVersion(1)},
		{event: &a2a.Message{ID: "test-event2"}, version: taskstore.TaskVersion(2)},
	}

	mustWrite(t, writeQueue, want...)

	if err := qm.Destroy(ctx, tid); err != nil {
		t.Fatalf("qm.Destroy() error = %v", err)
	}

	var got []*eventVersionPair
	for {
		msg, err := readQueue.Read(ctx)
		if errors.Is(err, ErrQueueClosed) {
			break
		}
		if err != nil {
			t.Fatalf("Read() error = %v", err)
		}
		got = append(got, &eventVersionPair{event: msg.Event, version: msg.TaskVersion})
	}
	if len(got) != len(want) {
		t.Fatalf("Read() got = %v, want %v", got, want)
	}
	for i, w := range want {
		if !reflect.DeepEqual(got[i].event, w.event) {
			t.Errorf("Read() got = %v, want %v", got, want)
		}
		if got[i].version != w.version {
			t.Errorf("Read() got version = %v, want %v", got[i].version, w.version)
		}
	}
}

func TestInMemoryQueue_ReadEmpty(t *testing.T) {
	t.Parallel()
	qm := newTestManager(t)
	tid := a2a.NewTaskID()

	readQueue, writeQueue := mustCreateReadWriter(t, qm, tid)
	completed := make(chan struct{})

	go func() {
		mustRead(t, readQueue)
		close(completed)
	}()

	select {
	case <-completed:
		t.Fatal("method should be blocking")
	case <-time.After(15 * time.Millisecond):
		// unblock blocked code by writing to queue
		mustWrite(t, writeQueue, newUnversioned(&a2a.Message{ID: "test"}))
	}
	<-completed
}

func TestInMemoryQueue_WriteFull(t *testing.T) {
	t.Parallel()
	qm := newTestManager(t, WithQueueBufferSize(1))
	tid := a2a.NewTaskID()

	readQueue, writeQueue := mustCreateReadWriter(t, qm, tid)
	completed := make(chan struct{})

	mustWrite(t, writeQueue, newUnversioned(&a2a.Message{ID: "1"}))
	go func() {
		mustWrite(t, writeQueue, newUnversioned(&a2a.Message{ID: "2"}))
		close(completed)
	}()

	select {
	case <-completed:
		t.Fatal("method should be blocking")
	case <-time.After(15 * time.Millisecond):
		// unblock blocked code by realising queue buffer
		mustRead(t, readQueue)
	}
	<-completed
}

func TestInMemoryQueue_WriteWithNoSubscribersDoesNotBlock(t *testing.T) {
	t.Parallel()
	qm := newTestManager(t, WithQueueBufferSize(0))
	tid := a2a.NewTaskID()

	writer, err := qm.CreateWriter(t.Context(), tid)
	if err != nil {
		t.Fatalf("qm.CreateWriter() error = %v", err)
	}
	mustWrite(t, writer, newUnversioned(&a2a.Message{ID: "test"}))
}

func TestInMemoryQueue_CloseUnsubscribesFromEvents(t *testing.T) {
	t.Parallel()
	qm := newTestManager(t, WithQueueBufferSize(0))
	ctx, tid := t.Context(), a2a.NewTaskID()

	readQueue, writeQueue := mustCreateReadWriter(t, qm, tid)

	if err := readQueue.Close(); err != nil {
		t.Fatalf("failed to close event queue: %v", err)
	}

	if err := writeQueue.Write(ctx, &Message{Event: &a2a.Message{ID: "test"}}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	msg, err := readQueue.Read(ctx)
	if !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("readQueue() = (%v, %v), want %v", msg, err, ErrQueueClosed)
	}
}

func TestInMemoryQueue_WriteWithCanceledContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())

	qm := newTestManager(t, WithQueueBufferSize(1))

	tid := a2a.NewTaskID()
	_, writeQueue := mustCreateReadWriter(t, qm, tid)

	// Fill the queue
	mustWrite(t, writeQueue, newUnversioned(&a2a.Message{ID: "1"}))
	cancel()

	err := writeQueue.Write(ctx, &Message{Event: &a2a.Message{ID: "2"}})
	if err == nil {
		t.Error("Write() with canceled context should have returned an error, but got nil")
	}
	if err != context.Canceled {
		t.Errorf("Write() error = %v, want %v", err, context.Canceled)
	}
}

func TestInMemoryQueue_BlockedWriteOnFullQueueThenDestroy(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	completed := make(chan struct{})

	qm := newTestManager(t, WithQueueBufferSize(1))

	tid := a2a.NewTaskID()
	_, writeQueue := mustCreateReadWriter(t, qm, tid)

	event := &a2a.Message{ID: "test"}

	// Fill the queue
	mustWrite(t, writeQueue, newUnversioned(&a2a.Message{ID: "1"}))

	go func() {
		err := writeQueue.Write(t.Context(), &Message{Event: event})
		if !errors.Is(err, ErrQueueClosed) {
			t.Errorf("Write() error = %v, want %v", err, ErrQueueClosed)
			return
		}
		close(completed)
	}()

	select {
	case <-completed:
		t.Fatal("method should be blocking")
	case <-time.After(20 * time.Millisecond):
		// unblock blocked code by closing queue
		err := qm.Destroy(ctx, tid)
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}
	<-completed
}

// stallBroker registers a subscriber that is never drained and never closed, and starts a
// writer that fills its buffer. That is what a streaming client which stopped reading
// without disconnecting looks like to the broker. It returns the abandoned subscriber.
func stallBroker(t *testing.T, qm Manager, tid a2a.TaskID, bufferSize int) Reader {
	t.Helper()
	ctx := t.Context()

	writer, err := qm.CreateWriter(ctx, tid)
	if err != nil {
		t.Fatalf("qm.CreateWriter() error = %v", err)
	}
	stalled, err := qm.CreateReader(ctx, tid)
	if err != nil {
		t.Fatalf("qm.CreateReader() error = %v", err)
	}

	go func() {
		for i := range bufferSize + 8 {
			event := &a2a.Message{ID: fmt.Sprintf("stall-%d", i)}
			if err := writer.Write(ctx, &Message{Event: event}); err != nil {
				return
			}
		}
	}()

	// Let the writer fill the abandoned subscriber's buffer, so that the broker is already
	// parked on the send to it by the time the assertions run.
	time.Sleep(100 * time.Millisecond)
	return stalled
}

// drainReader keeps reading a subscriber into a channel, so that the subscriber under
// assertion is never itself the one holding up the broker. The channel is closed once the
// subscriber stops delivering.
func drainReader(t *testing.T, reader Reader) <-chan *Message {
	t.Helper()
	ctx := t.Context()
	delivered := make(chan *Message, 64)
	go func() {
		defer close(delivered)
		for {
			msg, err := reader.Read(ctx)
			if err != nil {
				return
			}
			select {
			case delivered <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()
	return delivered
}

func TestInMemoryQueue_StalledSubscriberDoesNotBlockClose(t *testing.T) {
	t.Parallel()
	qm := newTestManager(t, WithQueueBufferSize(1))
	tid := a2a.NewTaskID()

	peer, err := qm.CreateReader(t.Context(), tid)
	if err != nil {
		t.Fatalf("qm.CreateReader() error = %v", err)
	}
	stallBroker(t, qm, tid, 1)

	closed := make(chan error, 1)
	go func() { closed <- peer.Close() }()

	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("peer.Close() error = %v, want nil", err)
		}
	case <-time.After(assertionTimeout):
		t.Fatalf("peer.Close() did not return within %v, want nil", assertionTimeout)
	}
}

func TestInMemoryQueue_StalledSubscriberDoesNotBlockNewSubscriber(t *testing.T) {
	t.Parallel()
	qm := newTestManager(t, WithQueueBufferSize(1))
	tid := a2a.NewTaskID()

	stallBroker(t, qm, tid, 1)

	created := make(chan error, 1)
	go func() {
		_, err := qm.CreateReader(t.Context(), tid)
		created <- err
	}()

	select {
	case err := <-created:
		if err != nil {
			t.Fatalf("qm.CreateReader() error = %v, want nil", err)
		}
	case <-time.After(assertionTimeout):
		t.Fatalf("qm.CreateReader() did not return within %v, want a reader", assertionTimeout)
	}
}

func TestInMemoryQueue_StalledSubscriberIsDroppedAndOthersResume(t *testing.T) {
	t.Parallel()
	qm := newTestManager(t, WithQueueBufferSize(1), WithSlowSubscriberTimeout(100*time.Millisecond))
	tid := a2a.NewTaskID()

	peer, err := qm.CreateReader(t.Context(), tid)
	if err != nil {
		t.Fatalf("qm.CreateReader() error = %v", err)
	}
	delivered := drainReader(t, peer)

	stalled := stallBroker(t, qm, tid, 1)

	const wantEvents = 5
	for i := range wantEvents {
		select {
		case _, ok := <-delivered:
			if !ok {
				t.Fatalf("peer.Read() delivered %d events, want %d", i, wantEvents)
			}
		case <-time.After(assertionTimeout):
			t.Fatalf("peer.Read() delivered %d events within %v, want %d", i, assertionTimeout, wantEvents)
		}
	}

	// The dropped subscriber may still drain what it buffered, then reports the drop.
	readCtx, cancel := context.WithTimeout(t.Context(), assertionTimeout)
	defer cancel()
	for {
		_, err := stalled.Read(readCtx)
		if errors.Is(err, ErrQueueClosed) {
			return
		}
		if err != nil {
			t.Fatalf("stalled.Read() error = %v, want %v", err, ErrQueueClosed)
		}
	}
}

func TestInMemoryQueue_StalledSubscriberBlocksWhenDroppingDisabled(t *testing.T) {
	t.Parallel()
	qm := newTestManager(t, WithQueueBufferSize(1), WithSlowSubscriberTimeout(0))
	tid := a2a.NewTaskID()

	peer, err := qm.CreateReader(t.Context(), tid)
	if err != nil {
		t.Fatalf("qm.CreateReader() error = %v", err)
	}
	delivered := drainReader(t, peer)

	stallBroker(t, qm, tid, 1)

	// With dropping disabled the queue keeps its unbounded blocking backpressure, so the
	// stalled subscriber does hold up dispatch. This is what makes the test above an
	// assertion about dropping rather than about the buffer being large enough.
	const wantEvents = 5
	deadline := time.After(500 * time.Millisecond)
	for i := range wantEvents {
		select {
		case <-delivered:
		case <-deadline:
			return
		}
		if i == wantEvents-1 {
			t.Fatalf("peer.Read() delivered %d events, want the stalled subscriber to hold up dispatch", wantEvents)
		}
	}
}
