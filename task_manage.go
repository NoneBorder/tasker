package tasker

import (
	"errors"
)

// RegisteredTask is a map record all consumer task
var RegisteredTask map[string]MsgQ

// RunningTaskChannel is a map record all running task
var RunningTaskChannel map[string]chan bool

func init() {
	RegisteredTask = make(map[string]MsgQ)
	RunningTaskChannel = make(map[string]chan bool)
}

// RegisterTask is used for consumer task at init func, register them self to the tasker
func RegisterTask(item MsgQ) {
	itemName := item.Topic()
	if _, exists := RegisteredTask[itemName]; exists {
		panic(errors.New("exists consumer topic " + itemName))
	}
	RegisteredTask[itemName] = item
	RunningTaskChannel[itemName] = make(chan bool, item.Concurency())
}

// InitAllTask will init all RegisteredTask to beego toolbox, this will start consume task
func InitAllTask() {
	for _, item := range RegisteredTask {
		MsgQInitTask(item)
	}
}
