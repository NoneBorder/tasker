package tasker

import (
	"errors"
)

var RegisteredTask map[string]MsgQ

func init() {
	RegisteredTask = make(map[string]MsgQ)
}

func RegisterTask(item MsgQ) {
	itemName := item.Topic()
	if _, exists := RegisteredTask[itemName]; exists {
		panic(errors.New("exists consumer topic " + itemName))
	}
	RegisteredTask[itemName] = item
}

func InitAllTask() {
	for _, item := range RegisteredTask {
		MsgQInitTask(item)
	}
}
