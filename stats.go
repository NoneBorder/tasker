package tasker

import (
	"time"

	"github.com/NoneBorder/dora"
)

// Stats is a interface for tasker to stat task execution result
type Stats interface {
	Stat(topic, status string, t time.Time, duration time.Duration) error
}

var stats Stats

// SetStats is used for specify a real stat implemention
func SetStats(s Stats) {
	stats = s
}

func Stat(topic, status string, duration time.Duration) {
	if stats == nil {
		return
	}

	now := time.Now().Local()
	if err := stats.Stat(topic, status, now, duration); err != nil {
		dora.Error().Msgf("[tasker] stat task error: %s", err.Error())
	}
}
