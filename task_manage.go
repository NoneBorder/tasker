package tasker

import (
	"errors"
)

// RegisteredTask is a map record all consumer task
var RegisteredTask map[string]MsgQ

func init() {
	RegisteredTask = make(map[string]MsgQ)
}

// RegisterTask is used for consumer task at init func, register them self to the tasker
func RegisterTask(item MsgQ) {
	itemName := item.Topic()
	if _, exists := RegisteredTask[itemName]; exists {
		panic(errors.New("exists consumer topic " + itemName))
	}
	RegisteredTask[itemName] = item
}

// InitAllTask will init all RegisteredTask to beego toolbox, this will start consume task
func InitAllTask() {
	for _, item := range RegisteredTask {
		MsgQInitTask(item)
	}
}
