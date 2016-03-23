// Command logtailer is designed to process log files for consumption.
//
// When provided a file for the `log_file` argument it invokes logtail2 to
// peform log file checkpointing including rotation detection.
//
// The first argument must be the profile identifier.
//
// When running as with `-` for the `log_file` argument it consumes stdin.
//
// Example invokation via cron:
//
//	* * * * * /usr/bin/logtailer nginx -log_file=/mnt/log/nginx/access.log
//
// See profile_dummy.go for an example of adding your own profile.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"syscall"

	"github.com/ParsePlatform/logtailer"
	"github.com/ParsePlatform/logtailer/profiles"
	"github.com/ParsePlatform/logtailer/profiles/dummy"
	"github.com/ParsePlatform/logtailer/profiles/mongodb"
	"github.com/ParsePlatform/logtailer/profiles/sshd"
)

var (
	logFile    = flag.String("log_file", "", "The input log file to consume.")
	stateDir   = flag.String("state_dir", "/var/run/logtailer", "The directory that will hold log tailing state.")
	dryRun     = flag.Bool("dry_run", false, "If True, will only print to stdout and will not update any state.")
	numWorkers = flag.Int("num_workers", 1, "Number of processing goroutines to run (1 for sequential).")
	goMaxProcs = flag.Int("gomaxprocs", runtime.NumCPU(), "Sets the number of os threads that will be utilized")
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %[1]s:\n\t%[1]s profile_name [arguments]\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "Available profiles: %s\n\nArguments:\n", profileNames())
	flag.PrintDefaults()
}

// availableProfiles defines the registered logtailer profiles.
// To add a new profile you must add it to this map.
// TODO(tredman): convert mysql, nginx, and haproxy tailers
var availableProfiles = map[string]profiles.Profile{
	"dummy":   new(dummy.DummyProfile),
	"mongodb": new(mongodb.MongodbProfile),
	"sshd":    new(sshd.SshdProfile),
}

func main() {
	// TODO: pick a better logger with support for Info, Debug, etc
	logger := log.New(os.Stderr, "DEBUG: ", log.LstdFlags|log.Lshortfile)
	flag.Usage = usage

	if len(os.Args) == 1 {
		flag.Usage()
		logger.Fatalln("No profile specified.")
	}

	profileName := os.Args[1]
	p, ok := availableProfiles[profileName]
	if !ok {
		flag.Usage()
		logger.Fatalln(fmt.Sprintf("Invalid profile '%s' profile selected.\n", profileName))
	}

	flag.CommandLine.Parse(os.Args[2:])

	runtime.GOMAXPROCS(*goMaxProcs)

	if *logFile == "" {
		flag.Usage()
		logger.Fatalln("No log file specified (-log_file argument).")
	}

	tailer := logtailer.NewLogtailer(p, *logFile, *stateDir, logger)
	tailer.DryRun = *dryRun

	if err := tailer.PrepEnvironment(); err != nil {
		logger.Fatalln("logtailer: issue with environment: ", err)
	}

	// on first first TERM/INT, stop the tailer and the signal handler
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-ch
		signal.Stop(ch)
		tailer.Stop()
	}()

	stats, err := tailer.Run(*numWorkers)
	if err != nil {
		logger.Fatalln("error in run: ", err)
	}
	fmt.Fprintln(os.Stderr, stats)
}

func profileNames() string {
	names := []string{}
	for k := range availableProfiles {
		names = append(names, k)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}
