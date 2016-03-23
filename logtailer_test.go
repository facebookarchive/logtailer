package logtailer

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/ParsePlatform/logtailer/profiles/dummy"
)

func ExampleNewLogtailer() {
	tmpFile, _ := ioutil.TempFile("", "")
	defer os.Remove(tmpFile.Name())

	logger := log.New(os.Stderr, "logtailer", log.LstdFlags)
	tailer := NewLogtailer(&dummy.DummyProfile{}, tmpFile.Name(), "/tmp/", logger)
	stats, _ := tailer.Run(1)
	fmt.Println(stats)
	// output:
	// {"Records":0,"ParseErrors":0,"SendErrors":0}
}
