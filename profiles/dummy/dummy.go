// Package dummy implements a dummy skeleton logtailer profile for demonstration
// purposes.
//
// This profile does not modify input lines and simply prints them to stdout.
package dummy

import "fmt"

// DummyProfile provides a stripped down example of how to write a logtailer profile.
type DummyProfile struct{}

// Init does nothing in the dummy profile, but is here to satisfy the interface
func (p *DummyProfile) Init() error {
	return nil
}

// Name returns the name of the profile and must be unique amongst registered.
// profiles
func (p *DummyProfile) Name() string {
	return "dummy"
}

// ProcessRecord is invoked for every input log line. It returns a transformed.
// line or an error
func (p *DummyProfile) ProcessRecord(record string) (interface{}, error) {
	return []byte(record), nil
}

// HandleOutput recieves a channel of input lines and a flag of whether or not
// this is a dry run being invoked (to avoid side-effects).
//
// The return value is a channel of errors. parse.com/logtailer keeps track of
// the number of errors and exits non-zero if they are over a threshold.
func (p *DummyProfile) HandleOutput(records <-chan interface{}, dryRun bool) <-chan error {

	// Set up the error channel.
	errChan := make(chan error)

	// Launch the consumption goroutine.
	go func() {

		// Close errChan when this goroutine finishes to signal being done.
		defer close(errChan)

		// Consume lines input channel until it is closed (if consuming stdin),
		// this is potentially never.
		for record := range records {
			line, ok := record.([]byte)
			if !ok {
				errChan <- fmt.Errorf("Unexpected output record type: %t", record)
				continue
			}
			fmt.Println(string(line))
		}
	}()

	return errChan
}
