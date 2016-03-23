// Package profiles describes the logtailer Profile interface and provides a
// simple registry
package profiles

// A Profile is a log consumer.
type Profile interface {
	// The unique identifier for the profile.
	Name() string
	// ProcessRecord processes a single input line from the input log file and returns
	// the parsed result
	ProcessRecord(record string) (result interface{}, err error)
	// HandleOutput is called for each result returned by ProcessRecord
	// This is where you should direct output (stdout, some http API, etc)
	HandleOutput(records <-chan interface{}, dryRun bool) (errors <-chan error)

	// Init is called just before parsing begins. This is where any setup specific
	// to your profile should go
	Init() error
}
