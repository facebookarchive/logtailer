package logtailer

import (
	"fmt"
	"os"
	"os/exec"
)

// logtailBinary is the name of the binary that will be invoked to fetch new log
// lines.
const logtailBinary = "logtail2"

// PrepEnvironment ensures the specified log file and state directories exist.
func (lt *Logtailer) PrepEnvironment() error {
	if lt.LogFile == "-" {
		return nil
	}
	_, err := os.Stat(lt.LogFile)
	if err != nil {
		return err
	}
	os.MkdirAll(lt.StateDir, 0644)
	_, err = os.Stat(lt.StateDir)
	if err != nil {
		return err
	}
	if _, err = exec.LookPath(logtailBinary); err != nil {
		return fmt.Errorf("could not find %s on PATH: %v", logtailBinary, err)
	}
	return nil
}
