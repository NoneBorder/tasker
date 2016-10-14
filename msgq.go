package tasker

import (
	"encoding/json"

	"github.com/astaxie/beego"
	"github.com/astaxie/beego/toolbox"
)

type MsgQ interface {
	New() MsgQ
	Topic() string
	TaskSpec() string
	Exec(uint64) error
}

func MsgQInitTask(m MsgQ) {
	task := toolbox.NewTask(m.Topic(), m.TaskSpec(), func() error {
		return MsgQConsume(m)
	})
	toolbox.AddTask(m.Topic(), task)
}

func MsgQPublish(m MsgQ) error {
	input, err := json.Marshal(m)
	if err != nil {
		return err
	}

	return NewSimpleTask(m.Topic(), string(input)).Publish()
}

func MsgQConsume(m MsgQ) error {
	num, err := Consume(m.Topic(), func(input string, workerID uint64) (err error) {
		this := m.New()
		if err = json.Unmarshal([]byte(input), this); err != nil {
			return
		}

		return this.Exec(workerID)
	})

	beego.BeeLogger.Info("consume for %s exec tasks=%d, err=%v", m.Topic(), num, err)
	return err
}
