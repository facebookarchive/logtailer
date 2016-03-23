// Package logtailer provides an easy way to write log file munging programs
package logtailer

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"github.com/ParsePlatform/logtailer/profiles"
)

// Logtailer holds the state of a logtailer program which represents the
// consumption of an input source with a particular profile
type Logtailer struct {
	Logger   *log.Logger
	Profile  profiles.Profile
	LogFile  string
	StateDir string
	DryRun   bool

	shutdown   chan struct{}
	logtailCmd *exec.Cmd
}

// Splitter supplies a custom function for a bufio.Scanner
type Splitter interface {
	Split(data []byte, atEOF bool) (advance int, token []byte, err error)
}

// NewLogtailer prepares a new Logtailer from a profile, input logfile, state
// directory, and a logger.
func NewLogtailer(profile profiles.Profile, logFile string, stateDir string, logger *log.Logger) *Logtailer {
	return &Logtailer{
		Logger:   logger,
		Profile:  profile,
		LogFile:  logFile,
		StateDir: stateDir,
		shutdown: make(chan struct{}),
	}
}

func (lt *Logtailer) getInput() (io.Reader, error) {
	if lt.LogFile == "-" {
		return os.Stdin, nil
	}

	lt.newLogtailCmd()

	// set up output pipeline.
	stdout, err := lt.logtailCmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("error connecting to logtail stdout: %v", err)
	}
	lt.logtailCmd.Stderr = os.Stderr
	if err := lt.logtailCmd.Start(); err != nil {
		return nil, fmt.Errorf("error starting logtail cmd: %v", err)
	}

	return stdout, nil
}

// Run starts the consumption of the input source and starts `numWorkers`
// separate goroutines to process lines.
//
// If the log lines are ordered `numWorkers` should be 1.
func (lt *Logtailer) Run(numWorkers int) (*Stats, error) {
	input, err := lt.getInput()
	stats := &Stats{}

	if err != nil {
		lt.Logger.Println("error getting logtail input:", err)
		return stats, err
	}
	scanner := bufio.NewScanner(input)
	inputRecords := make(chan string)
	outputRecords := make(chan interface{})

	// run any initialization routines needed by the profile
	err = lt.Profile.Init()
	if err != nil {
		return stats, err
	}

	// start scanner goroutine
	go func() {
		defer close(inputRecords)
		// if the profile supplies a custom splitting function, use it
		if splitter, ok := lt.Profile.(Splitter); ok {
			scanner.Split(splitter.Split)
		}

		for scanner.Scan() {
			// hand every token to inputRecords to be consumed by the profile
			stats.Records++
			select {
			case inputRecords <- scanner.Text():
			case <-lt.shutdown:
				return
			}
		}
	}()
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				line, ok := <-inputRecords
				if !ok {
					return
				}
				record, err := lt.Profile.ProcessRecord(line)

				if err != nil {
					lt.Logger.Println("error parsing:", err)
					stats.Lock()
					stats.ParseErrors++
					stats.Unlock()
				} else {
					outputRecords <- record
				}
			}
		}()
	}

	errorChan := lt.Profile.HandleOutput(outputRecords, lt.DryRun)

	go func() {
		for err := range errorChan {
			stats.Lock()
			stats.SendErrors++
			stats.Unlock()
			lt.Logger.Println("error sending:", err)
		}
	}()
	wg.Wait()
	close(outputRecords)

	if stats.IsHealthy() {
		err = nil
	} else {
		err = fmt.Errorf("stats indicate unhealthy run: %+v", stats)
	}
	return stats, err
}

// Stop stops consuming new input
func (lt *Logtailer) Stop() {
	close(lt.shutdown)
}

func (lt *Logtailer) stateFilePath() string {
	logFileName := filepath.Base(lt.LogFile)
	fileName := fmt.Sprintf("logtailer-%s-%s.state", lt.Profile.Name(), logFileName)
	return filepath.Join(lt.StateDir, fileName)
}

func (lt *Logtailer) newLogtailCmd() {
	args := []string{
		"-f", lt.LogFile,
		"-o", lt.stateFilePath(),
	}
	if lt.DryRun {
		args = append(args, "-t")
	}
	lt.Logger.Println("executing ", logtailBinary, args)
	lt.logtailCmd = exec.Command(logtailBinary, args...)
}
