# logtailer 

## Summary 

A simple log tailer written in go. Originally written by Parse to consume production log data of various formats and feed it into Facebook's analytics systems for day-to-day operations. logtailer uses a modular approach to consuming logs and directing output. To support new log types or change existing behavior, simply implement the Profile interface to suit your needs. The reference implementations in this release consume logs directly and output parsed lines as stdout.

Reference implementations include:

* a dummy profile used for demonstration. Consumes the input log file and prints to stdout
* a mongodb log parser based on a Programmable Expression Grammar (PEG). At Parse we found the PEG parser to perform better, and more accurately, than any regex-based pattern we could come up with, due to the complex nature of MongoDB log lines. The PEG parser focuses on actual operations (queries, inserts, commands, etc) and ignores other noise. At Parse, we processed 4B operations/day with this tailer. The mongodb tailer converts lines into a consistent JSON format that can be processed by other analytics systems.
* an sshd log parser that converts ssh login events to JSON

## Building

Has been tested on go version 1.5.3, but will probably work with earlier versions.

```sh
$ go install github.com/ParsePlatform/logtailer/cmd/logtailer
```

## External Dependencies

Logtailer was written to run cron once per minute. Since logs rotate less frequently than that, it relies on the [logtail2](http://manpages.ubuntu.com/manpages/trusty/man8/logtail2.8.html) command readily available in the Ubuntu repositories. When repeatedly run with the same log file for input, logtail2 ensures that only new lines are consumed. To make this work, ensure that the logtailer run directory exists:

```sh
mkdir -p /var/run/logtailer
chown <your tailer user> /var/run/logtailer
```

Alternatively, logtailer accepts stdin as input. Simply specify *-* to the *log_file* flag when invoking logtailer.

## Testing

The simplest test of the binary is to invoke the *dummy* profile with some simple input. It should be echoed back, along with some statistics that go to stderr.

```sh
echo -n '1\n2\n3\n' | ./logtailer dummy -log_file -
1
2
3
{"Records":3,"ParseErrors":0,"SendErrors":0}
```
## Tuning

By default, the *logtailer* creates a worker for each CPU on the system. You can override this by setting the *num_workers* flag.
