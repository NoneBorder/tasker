# Tasker

A light distribute producer&consumer task model based on beego.

## Features

* Custom task input, execute logic
* Auto retry after task execute failed

## Limited

* based on beego orm

## Installation

`go get github.com/NoneBorder/tasker`

## Usage

### Register to beego model

```go
...
orm.RegisterModel(new(tasker.Task))
...
```

### Create your task struct

```go
type ExampleTask struct {
    Name string
}

func (self *ExampleTask) New() tasker.MsgQ {
    return &ExampleTask{}
}

func (self *ExampleTask) Topic() string {
    return "example_task"
}

func (self *ExampleTask) TaskSpec() string {
    // beego task spec format
    return "*/1 * * * * *"
}

func (self *ExampleTask) Exec(workerID uint64) error {
    fmt.Println(self.Name)
    return nil
}
```

### Publish task

```go
tasker.MsgQPublish(&ExampleTask{
    Name: "example test",
})
```

### Consume task

* control consume your self
```go
tasker.MsgQConsume(new(ExampleTask))
```

* consumer generate as taskspec
```go
tasker.MsgQInitTask(new(ExampleTask))
```

## Advanced Config

### OS ENV

* **NB_TASKER_JUST_WORKER**: the instance will not race to become master when the environment set to "ON"

## License

The MIT License (MIT)
