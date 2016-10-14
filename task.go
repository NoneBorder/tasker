// Package tasker implements a simple library for distribute tasks.

package tasker

import (
	"errors"
	"time"

	"github.com/astaxie/beego"
	"github.com/astaxie/beego/orm"
	"github.com/sony/sonyflake"
)

/*
                                   +----> failed
                                   |
   pending ----+----> running -----+----> success
               ^                   |
               |                   v
               +---------------- retry
*/
const (
	TaskStatPending = "pending"
	TaskStatRunning = "running"
	TaskStatRetry   = "retry"
	TaskStatFailed  = "failed"
	TaskStatSuccess = "success"
)

type Task struct {
	Id       int
	Topic    string
	Status   string
	Timeout  int // 超时ms
	Retry    int
	Input    string `orm:"type(text)"`
	WorkerId uint64
	Created  time.Time `orm:"auto_now_add"`
	Updated  time.Time `orm:"auto_now"`
	Log      string    `orm:"type(text)"`
}

// The consume func definition
type ConsumeFn func(Input string, WorkerID uint64) (err error)

// use for generate worker id
var UniqID *sonyflake.Sonyflake

func init() {
	// init sonyflake
	UniqID = sonyflake.NewSonyflake(sonyflake.Settings{
		StartTime: time.Now(),
	})
}

func (self *Task) TableEngine() string {
	return "INNODB"
}

// New task with default settings
func NewSimpleTask(topic, input string) *Task {
	return &Task{
		Topic:    topic,
		Status:   "pending",
		Timeout:  5000,
		Retry:    3,
		Input:    input,
		WorkerId: 0,
	}
}

// Publish Task to Msg Queue
func (self *Task) Publish() (err error) {
	o := orm.NewOrm()
	_, err = o.Insert(self)
	return
}

func (self *Task) exec(fn ConsumeFn) (err error) {
	errC := make(chan error)
	go func() {
		errC <- fn(self.Input, self.WorkerId)
	}()

	select {
	case err = <-errC:
	case <-time.After(time.Duration(self.Timeout) * time.Millisecond):
		err = errors.New("execute task timeout")
	}

	return
}

func (self *Task) consume(fn ConsumeFn) bool {
	err := self.exec(fn)
	success := err == nil

	if !success {
		// failed
		self.Log += err.Error() + "\n"
		self.Status = TaskStatFailed
		self.Retry -= 1
		if self.Retry > 0 {
			self.Status = TaskStatRetry
		}
	} else {
		self.Status = TaskStatSuccess
	}

	self.WorkerId = 0
	o := orm.NewOrm()
	for i := 0; i < 3; i++ {
		if _, err := o.Update(self); err != nil {
			beego.BeeLogger.Error("set task state failed: err=%s, task=%s", err.Error(), self)
		} else {
			break
		}
	}

	return success
}

// Consume a topic task
func Consume(topic string, fn ConsumeFn, concurency ...int) (int, error) {
	concurency = append(concurency, 1)

	workerID, err := UniqID.NextID()
	if err != nil {
		return 0, err
	}
	o := orm.NewOrm()

	res, err := o.Raw(`UPDATE task SET status=?, worker_id=? WHERE
		topic=? AND worker_id=0 AND status IN (?,?) LIMIT ?`,
		TaskStatRunning, workerID, topic, TaskStatPending, TaskStatRetry, concurency[0],
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

	waitChannel := make(chan bool)
	for _, t := range tasks {
		go func(t *Task) {
			waitChannel <- t.consume(fn)
		}(t)
	}

	successTasks := 0
	for _ = range tasks {
		if <-waitChannel {
			successTasks += 1
		}
	}

	return successTasks, nil
}
