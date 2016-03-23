// Package sshd parses ssh log lines and generates JSON representing ssh events
package sshd

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"time"
)

// SshdProfile is a logtailer profile that parses ssh login events from sshd logs
type SshdProfile struct {
	// maps key fingerprints to fb users
	fingerprintToFbUser map[string]string

	// events is the in-flight ssh events that are being built up
	events map[string]*sshEvent

	// completeEvents is populated with finished events
	completeEvents chan *sshEvent

	// Logger is used to report tailer issues to stderr
	logger *log.Logger
}

// sshEvent represents a successful ssh login event
type sshEvent struct {
	Timestamp   time.Time `json:"timestamp,omitempty"`
	Logtime     time.Time `json:"logtime,omitempty"`
	PeType      string    `json:"pe_type,omitempty"`
	Hostname    string    `json:"hostname,omitempty"`
	Pid         int       `json:"pid,omitempty"`
	DstIP       string    `json:"dst_ip,omitempty"`
	SrcIP       string    `json:"src_ip,omitempty"`
	Port        int       `json:"port,omitempty"`
	Fingerprint string    `json:"fingerprint,omitempty"`
	DstUser     string    `json:"dst_user,omitempty"`
	FbUser      string    `json:"fb_user,omitempty"`
	ChildPid    int       `json:"child_pid,omitempty"`
	Success     bool      `json:"success"`
	FailReason  string    `json:"fail_reason,omitempty"`

	Complete bool `json:"complete,omitempty"`
}

func (e *sshEvent) ID() string {
	return e.Hostname + ":" + strconv.Itoa(e.Pid)
}

func (e *sshEvent) String() string {
	buf, _ := json.Marshal(&e)
	return string(buf)
}

// string types to look for in ssh logs
var (
	timeFormat = "Jan 02 15:04:05"

	sshLogRe           = regexp.MustCompile(`^([A-z]{3} [0-9]+ [^ ]+) ([a-zA-Z0-9-]+) sshd\[(\d+)\]: (.*)`)
	connectLineRe      = regexp.MustCompile(`^Connection from ([0-9.]+) port (\d+)$`)
	foundKeyRe         = regexp.MustCompile(`^Found matching RSA key: ([0-9a-f:]+)`)
	acceptKeyRe        = regexp.MustCompile(`^Accepted publickey for (\w+) from ([0-9.]+) port (\d+) ssh2`)
	childPidRe         = regexp.MustCompile(`^User child is on pid (\d+)`)
	badRevMapRe        = regexp.MustCompile(`^reverse mapping checking getaddrinfo .*`)
	failedPubKeyRe     = regexp.MustCompile(`^Failed publickey for (\w+) from ([0-9.]+) port (\d+)`)
	connectionClosedRe = regexp.MustCompile(`^Connection closed by ([0-9.]+) \[(.*)\]`)
)

// Name returns the name of the profile and must be unique amongst registered.
// profiles
func (p *SshdProfile) Name() string {
	return "sshd"
}

// Init initializes the SshdProfile instance
func (p *SshdProfile) Init() error {
	p.logger = log.New(os.Stderr, "DEBUG: ", log.LstdFlags|log.Lshortfile)
	p.completeEvents = make(chan *sshEvent)
	p.events = make(map[string]*sshEvent)
	p.fingerprintToFbUser = populateKeyMapping()
	return nil
}

// ProcessRecord is invoked for every input log line. It returns a transformed.
// line or an error
func (p *SshdProfile) ProcessRecord(line string) (interface{}, error) {
	ok := sshLogRe.MatchString(line)
	if !ok {
		// if it's not an sshd line, return error
		p.logger.Println("bad line:", line)
		return nil, errors.New("logtailer.sshd: unexpected log line not from sshd")
	}
	res := sshLogRe.FindStringSubmatch(line)
	partialEvent := sshEvent{}
	partialEvent.Timestamp = time.Now()
	partialEvent.Logtime, _ = time.Parse(timeFormat, res[1])

	partialEvent.Hostname = res[2]
	partialEvent.Pid, _ = strconv.Atoi(res[3])
	message := res[4]
	switch {
	case connectLineRe.MatchString(message):
		res = connectLineRe.FindStringSubmatch(message)
		partialEvent.PeType = "connectLine"
		partialEvent.SrcIP = res[1]
		partialEvent.Port, _ = strconv.Atoi(res[2])
	case foundKeyRe.MatchString(message):
		res = foundKeyRe.FindStringSubmatch(message)
		partialEvent.PeType = "foundKey"
		partialEvent.Fingerprint = res[1]
		partialEvent.FbUser = p.fingerprintToFbUser[res[1]]
	case acceptKeyRe.MatchString(message):
		res = acceptKeyRe.FindStringSubmatch(message)
		partialEvent.PeType = "acceptKey"
		partialEvent.DstUser = res[1]
		partialEvent.SrcIP = res[2]
		partialEvent.Port, _ = strconv.Atoi(res[3])
		partialEvent.Success = true
	case childPidRe.MatchString(message):
		res = childPidRe.FindStringSubmatch(message)
		partialEvent.PeType = "childPid"
		partialEvent.ChildPid, _ = strconv.Atoi(res[1])
		partialEvent.Success = true
	case badRevMapRe.MatchString(message):
		res = badRevMapRe.FindStringSubmatch(message)
		partialEvent.PeType = "badRevMap"
		partialEvent.FailReason = "reverse mapping checking getaddrinfo"
		partialEvent.Success = false
	case failedPubKeyRe.MatchString(message):
		res = failedPubKeyRe.FindStringSubmatch(message)
		partialEvent.PeType = "failedPubKey"
		partialEvent.Success = false
	case connectionClosedRe.MatchString(message):
		res = connectionClosedRe.FindStringSubmatch(message)
		partialEvent.PeType = "connectionClosed"
		partialEvent.SrcIP = res[1]
		partialEvent.FailReason = res[2]
		partialEvent.Success = false
	default:
		return []byte("{}"), nil
	}
	return json.Marshal(partialEvent)
}

func (p *SshdProfile) handlePartialEvent(partialEvent []byte) error {
	var event sshEvent
	if err := json.Unmarshal(partialEvent, &event); err != nil {
		return err
	}

	// check for existing event
	key := event.ID()
	fullEvent, ok := p.events[key]
	// if not present, insert new
	if !ok {
		fullEvent = &sshEvent{}
		p.events[key] = fullEvent
	}
	// if present, merge values, check if complete
	fullEvent.Timestamp = time.Now()
	fullEvent.Logtime = event.Logtime
	fullEvent.Hostname = event.Hostname
	fullEvent.Pid = event.Pid
	// TODO nuke this and replace it with a reflection that sets values in
	// fullEvent for all non-nil fields in event
	//
	// If we run with multiple workers we have a data race here and should move
	// Complete calculation after the switch
	switch event.PeType {
	case "connectLine":
		fullEvent.SrcIP = event.SrcIP
		fullEvent.Port = event.Port
	case "foundKey":
		fullEvent.Fingerprint = event.Fingerprint
		fullEvent.FbUser = event.FbUser
	case "acceptKey":
		fullEvent.DstUser = event.DstUser
		fullEvent.SrcIP = event.SrcIP
		fullEvent.Port = event.Port
		fullEvent.Success = event.Success
	case "childPid":
		fullEvent.ChildPid = event.ChildPid
		fullEvent.Success = event.Success
		fullEvent.Complete = true
	case "badRevMap":
		fullEvent.FailReason = event.FailReason
		fullEvent.Success = event.Success
	case "failedPubKey":
		fullEvent.Success = event.Success
		fullEvent.Complete = true
	case "connectionClosed":
		fullEvent.Success = event.Success
		fullEvent.SrcIP = event.SrcIP
		fullEvent.FailReason = event.FailReason
		fullEvent.Complete = true
	}
	// if complete
	if fullEvent.Complete {
		p.completeEvents <- fullEvent
		delete(p.events, key)
	}

	return nil
}

// HandleOutput recieves a channel of input lines and a flag of whether or not
// this is a dry run being invoked (to avoid side-effects).
//
// The return value is a channel of errors. logtailer keeps track of
// the number of errors and exits non-zero if they are over a threshold.
func (p *SshdProfile) HandleOutput(records <-chan interface{}, dryRun bool) <-chan error {
	errChan := make(chan error)
	timeoutCheckTicker := time.NewTicker(5 * time.Second)

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
			if err := p.handlePartialEvent(line); err != nil {
				errChan <- err
			}
		}
		// if we're closing up shop make sure the events cleaner goroutine stops too.
		timeoutCheckTicker.Stop()
	}()

	// expire events that aren't updated for 60s
	go func() {
		for {
			<-timeoutCheckTicker.C
			for key, event := range p.events {
				expireEventAt := event.Timestamp.Add(60 * time.Second)
				if time.Now().After(expireEventAt) {
					event.Success = false
					event.FailReason = "timeout waiting for complete event"
					p.completeEvents <- event
					delete(p.events, key)
				}
			}
		}
	}()

	go func() {
		// write events to stdout
		for event := range p.completeEvents {
			message := event.String()
			if len(message) > 0 {
				fmt.Println(message)
			}
		}
	}()

	return errChan
}
