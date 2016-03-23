package logtailer

import (
	"encoding/json"
	"sync"
)

// Stats holds basic metrics about the logtailer run.
type Stats struct {
	Records     int
	ParseErrors int
	SendErrors  int
	sync.Mutex
}

// what percentage of records must be parsed successfully to be considered healthy.
var acceptableParseFailRatio = 0.1

// IsHealthy returns true if the stats appear healthy.
func (s *Stats) IsHealthy() bool {
	if s.SendErrors > 0 {
		return false
	}

	if s.Records > 0 && s.ParseErrors > 0 {
		if float64(s.ParseErrors)/float64(s.Records) > acceptableParseFailRatio {
			return false
		}
	}
	return true
}

func (s *Stats) String() string {
	buf, _ := json.Marshal(s)
	return string(buf)
}
