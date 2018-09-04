package tasker

import (
	"context"
	"encoding/json"
	"time"

	"github.com/NoneBorder/dora"
	"github.com/astaxie/beego/toolbox"
)

var zeroTime, _ = time.Parse(time.RFC3339, "1971-01-01T00:00:00Z00:00")

// MsgQ is interface for msg, all messages should be implements these interfaces.
type MsgQ interface {
	New() MsgQ
	Topic() string
	TaskSpec() string
	Concurency() int
	Exec(*context.Context, uint64) error
}

// MsgQInitTask add consume task to beego toolbox task, run consumer interval.
func MsgQInitTask(m MsgQ) {
	task := toolbox.NewTask(m.Topic(), m.TaskSpec(), func() error {
		return MsgQConsume(m)
	})
	toolbox.AddTask(m.Topic(), task)
}

// MsgQPublish represents publish a task.
func MsgQPublish(m MsgQ) error {
	input, err := json.Marshal(m)
	if err != nil {
		return err
	}

	return NewSimpleTask(m.Topic(), string(input)).Publish()
}

// MsgQPublishWithRetry represents publish with retry and timeout set
func MsgQPublishWithRetry(m MsgQ, timeout time.Duration, retry int) error {
	return MsgQPublishWithPlanTime(m, timeout, retry, zeroTime)
}

func MsgQPublishWithPlanTime(m MsgQ, timeout time.Duration, retry int, execAfter time.Time) error {
	input, err := json.Marshal(m)
	if err != nil {
		return err
	}

	task := &Task{
		Topic:     m.Topic(),
		Status:    TaskStatPending,
		Timeout:   int(timeout.Seconds() * 1000),
		ExecAfter: execAfter,
		Retry:     retry,
		Input:     string(input),
		WorkerId:  0,
	}

	return task.Publish()
}

// MsgQConsume is for consume a message type.
func MsgQConsume(m MsgQ) error {
	num, err := Consume(m.Topic(), func(ctx *context.Context, input string, workerID uint64) (err error) {
		this := m.New()
		if err = json.Unmarshal([]byte(input), this); err != nil {
			return
		}

		return this.Exec(ctx, workerID)
	})

	dora.Info().Msgf("consume for %s exec tasks=%d, err=%v", m.Topic(), num, err)
	return err
}
