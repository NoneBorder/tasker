package tasker

import (
	"context"
	"errors"
	"time"

	"github.com/NoneBorder/dora"
	"github.com/astaxie/beego/orm"
)

const (
	TaskStatPending = "pending"
	TaskStatRunning = "running"
	TaskStatRetry   = "retry"
	TaskStatFailed  = "failed"
	TaskStatSuccess = "success"
)

const (
	TaskCtxKeyLog       = "stdout"          // normal log record
	TaskCtxKeyExecAfter = "task-exec-after" // plan task exec time
)

// Task is the core object for tasker package.
type Task struct {
	Id         uint64
	Topic      string
	Status     string
	Timeout    int // 超时ms
	ExecAfter  time.Time `orm:"type(datetime);default(1971-01-01 00:00:00)"`
	Retry      int
	Input      string    `orm:"type(text)"`
	WorkerId   uint64
	WorkerFQDN string    `orm:"column(worker_fqdn);type(text)"`
	Created    time.Time `orm:"auto_now_add"`
	Updated    time.Time `orm:"auto_now"`
	Log        string    `orm:"type(text)"`
	Errlog     string    `orm:"type(text)"`
}

// ConsumeFn represents consume func definition.
type ConsumeFn func(ctx *context.Context, Input string, WorkerID uint64) (err error)

func (self *Task) TableIndex() [][]string {
	return [][]string{
		[]string{"Topic", "Status"},
	}
}

func (self *Task) TableEngine() string {
	return "INNODB"
}

// NewSimpleTask is new task with default settings.
func NewSimpleTask(topic, input string) *Task {
	return &Task{
		Topic:     topic,
		Status:    TaskStatPending,
		Timeout:   5000,
		ExecAfter: zeroTime,
		Retry:     3,
		Input:     input,
		WorkerId:  0,
	}
}

// Publish Task to Msg Queue.
func (self *Task) Publish() (err error) {
	o := orm.NewOrm()
	if _, err = o.Insert(self); err == nil {
		Stat(self.Topic, TaskStatPending, time.Duration(0))
	}
	return
}

func (self *Task) exec(ctx *context.Context, fn ConsumeFn) (err error) {
	// errC in select no other goroutine, must use buffered channel
	errC := make(chan error, 1)
	select {
	case errC <- fn(ctx, self.Input, self.WorkerId):
		err = <-errC

	case <-(*ctx).Done():
		err = (*ctx).Err()
	}

	return
}

func (self *Task) consume(fn ConsumeFn) bool {
	startT := time.Now().Local()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(self.Timeout+1)*time.Millisecond)
	defer cancel()

	err := self.exec(&ctx, fn)
	success := err == nil

	if !success {
		// failed
		self.Errlog += err.Error() + "\n"
		self.Status = TaskStatFailed
		self.Retry -= 1
		if self.Retry > 0 {
			self.Status = TaskStatRetry

			// when the job will retry, you can update the next exec time
			if execAfter, ok := ctx.Value(TaskCtxKeyExecAfter).(time.Time); ok {
				self.ExecAfter = execAfter
			}
		}
	} else {
		self.Status = TaskStatSuccess
	}

	// stat task exec
	Stat(self.Topic, self.Status, time.Since(startT))

	// workerid set to 0
	self.WorkerId = 0

	// set stdout log from ctx
	if log, ok := ctx.Value(TaskCtxKeyLog).(string); ok && log != "" {
		self.Log = log
	}

	// update db record
	o := orm.NewOrm()
	for i := 0; i < 3; i++ {
		if _, err := o.Update(self); err != nil {
			dora.Error().Msgf("set task state failed: err=%s, task=%s", err.Error(), self)
		} else {
			break
		}
	}

	return success
}

// Consume a topic task.
func Consume(topic string, fn ConsumeFn) (int, error) {
	o := orm.NewOrm()
	if IsMaster {
		// 仅在 master 上对已经僵死的 task 进行重置，减轻 DB 压力
		if _, err := o.Raw(`UPDATE task SET status=?, retry=retry-1, worker_id=0, updated=NOW() WHERE topic=? AND status=?
		AND TIMESTAMPDIFF(SECOND, updated, now())*1000-5000>timeout`,
			TaskStatRetry, topic, TaskStatRunning,
		).Exec(); err != nil {
			dora.Error().Msgf("update dead running task to waiting failed: %s", err.Error())
		}
	}

	// Consume operator should be atomic
	ConsumeMutex[topic].Lock()
	defer ConsumeMutex[topic].Unlock()

	maxConsumeTask := cap(RunningTaskChannel[topic]) - len(RunningTaskChannel[topic])
	if maxConsumeTask == 0 {
		return 0, errors.New("reached max running task limit")
	}

	workerID, err := UniqID.NextID()
	if err != nil {
		return 0, err
	}

	res, err := o.Raw(`UPDATE task SET status=?, worker_id=?, worker_fqdn=concat(worker_fqdn, ?), updated=NOW() WHERE
		topic=? AND worker_id=0 AND status IN (?,?) AND exec_after <= NOW() LIMIT ?`,
		TaskStatRunning, workerID, FQDN()+"\n", topic, TaskStatPending, TaskStatRetry, maxConsumeTask,
	).Exec()
	if err != nil {
		// not update db
		return 0, err
	}

	num, _ := res.RowsAffected()
	if num == 0 {
		// no task for consume
		return 0, nil
	}

	tasks := []*Task{}
	_, err = o.Raw("SELECT * FROM task WHERE `topic`=? AND `status`=? AND `worker_id`=?",
		topic, TaskStatRunning, workerID,
	).QueryRows(&tasks)
	if len(tasks) == 0 || err != nil {
		return 0, err
	}

	// TODO: 如何保证程序退出时正在运行的 task 可用？当前超时后 5s 僵尸 task 会被重新激活，但不能保证原子性
	for _, t := range tasks {
		RunningTaskChannel[topic] <- true
		go func(t *Task) {
			t.consume(fn)
			<-RunningTaskChannel[topic]
		}(t)
	}

	return len(tasks), nil
}

// ConsumeCtxStdout write stdout log to context for record
func ConsumeCtxStdout(ctx *context.Context, log string) {
	*ctx = context.WithValue(*ctx, TaskCtxKeyLog, log)
}

// ConsumeCtxExecAfter update run time for next retry
func ConsumeCtxExecAfter(ctx *context.Context, t time.Time) {
	*ctx = context.WithValue(*ctx, TaskCtxKeyExecAfter, t)
}
