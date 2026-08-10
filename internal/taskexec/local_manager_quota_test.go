// Copyright 2026 The A2A Authors
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

package taskexec

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/eventqueue"
	"github.com/a2aproject/a2a-go/v2/a2asrv/limiter"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"
	"github.com/a2aproject/a2a-go/v2/internal/eventpipe"
	"github.com/a2aproject/a2a-go/v2/internal/testutil"
)

type chattyExecutor struct {
	events int
}

func (e *chattyExecutor) Execute(ctx context.Context, queue eventpipe.Writer) error {
	for i := range e.events {
		event := &a2a.Message{ID: fmt.Sprintf("event-%d", i)}
		if err := queue.Write(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

type chattyProcessor struct {
	seen int
	max  int
}

func (p *chattyProcessor) Process(ctx context.Context, event a2a.Event) (*ProcessorResult, error) {
	p.seen++
	version := taskstore.TaskVersion(p.seen)
	if p.seen >= p.max {
		return &ProcessorResult{ExecutionResult: &a2a.Message{ID: "done"}, TaskVersion: version}, nil
	}
	return &ProcessorResult{TaskVersion: version}, nil
}

func (p *chattyProcessor) ProcessError(ctx context.Context, err error) (a2a.SendMessageResult, error) {
	return nil, err
}

func (p *chattyProcessor) Cleanup(ctx context.Context, result a2a.SendMessageResult, err error) {}

// TestLocalManager_AbandonedSubscriberDoesNotLeakConcurrencyQuota covers the reason the
// event queue drops a subscriber that stops reading. An execution is only cleaned up, and
// its concurrency-quota slot only released, once its events have been dispatched. A client
// that opens a stream and then stops reading therefore used to hold the slot forever, so a
// handful of them refused every new execution on the whole server.
func TestLocalManager_AbandonedSubscriberDoesNotLeakConcurrencyQuota(t *testing.T) {
	t.Parallel()

	const events = 8
	queueManager := eventqueue.NewInMemoryManager(
		eventqueue.WithQueueBufferSize(1),
		eventqueue.WithSlowSubscriberTimeout(300*time.Millisecond),
	)
	manager := NewLocalManager(LocalManagerConfig{
		QueueManager: queueManager,
		TaskStore:    testutil.NewTestTaskStore(),
		Factory: &testFactory{
			CreateExecutorFn: func(context.Context, a2a.TaskID, *a2a.SendMessageRequest) (Executor, Processor, Cleaner, error) {
				processor := &chattyProcessor{max: events}
				return &chattyExecutor{events: events}, processor, processor, nil
			},
		},
		ConcurrencyConfig: limiter.ConcurrencyConfig{MaxExecutions: 1},
	})

	// Execute creates a default subscription reader. Never calling Events() on the returned
	// subscription is what a client that opened a stream and stopped reading looks like.
	if _, err := manager.Execute(t.Context(), &a2a.SendMessageRequest{Message: &a2a.Message{ID: "first"}}); err != nil {
		t.Fatalf("manager.Execute() error = %v, want nil", err)
	}

	// The limit is one execution, so the second call is admitted only after the first one
	// has been cleaned up, which requires the abandoned subscriber to be dropped.
	deadline := time.Now().Add(3 * time.Second)
	var err error
	for time.Now().Before(deadline) {
		if _, err = manager.Execute(t.Context(), &a2a.SendMessageRequest{Message: &a2a.Message{ID: "second"}}); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("manager.Execute() error = %v, want nil within 3s", err)
}
