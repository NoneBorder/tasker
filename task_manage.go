package tasker

import (
	"errors"
	"sync"
)

// RegisteredTask is a map record all consumer task
var RegisteredTask map[string]MsgQ

// RunningTaskChannel is a map record all running task
var RunningTaskChannel map[string]chan bool

// ConsumeMutex is a map record consumer consume task lock
var ConsumeMutex map[string]sync.Mutex

func init() {
	RegisteredTask = make(map[string]MsgQ)
	RunningTaskChannel = make(map[string]chan bool)
	ConsumeMutex = make(map[string]sync.Mutex)
}

// RegisterTask is used for consumer task at init func, register them self to the tasker
func RegisterTask(item MsgQ) {
	itemName := item.Topic()
	if _, exists := RegisteredTask[itemName]; exists {
		panic(errors.New("exists consumer topic " + itemName))
	}
	RegisteredTask[itemName] = item
	RunningTaskChannel[itemName] = make(chan bool, item.Concurency())
	ConsumeMutex[itemName] = sync.Mutex{}
}

// InitAllTask will init all RegisteredTask to beego toolbox, this will start consume task
func InitAllTask() {
	for _, item := range RegisteredTask {
		MsgQInitTask(item)
	}
}
