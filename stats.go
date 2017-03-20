package tasker

import (
	"time"

	"github.com/astaxie/beego"
)

type Stats interface {
	Stat(topic, status string, t time.Time, duration time.Duration) error
}

var stats Stats

func SetStats(s Stats) {
	stats = s
}

func Stat(topic, status string, duration time.Duration) {
	if stats == nil {
		return
	}

	now := time.Now().Local()
	if err := stats.Stat(topic, status, now, duration); err != nil {
		beego.BeeLogger.Error("[tasker] stat task error: %s", err.Error())
	}
}
