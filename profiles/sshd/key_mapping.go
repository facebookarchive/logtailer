package sshd

import (
	"bufio"
	"bytes"
	"flag"
	"io/ioutil"
	"log"
	"strings"
)

var authorizedKeyPath = flag.String("authorized_keys_path", "/home/ubuntu/.ssh/authorized_keys", "path to authorized keys path to provide fingerprint to user mapping")

func populateKeyMapping() map[string]string {
	result := make(map[string]string)

	f, err := ioutil.ReadFile(*authorizedKeyPath)
	if err != nil {
		log.Println("logtailer.sshd error reading authorized_keys:", err)
		return result
	}
	scanner := bufio.NewScanner(bytes.NewBuffer(f))

	for scanner.Scan() {
		keyLine := scanner.Bytes()
		if len(keyLine) == 0 {
			continue
		}

		key, err := ParseAuthorizedKey(keyLine)
		if err != nil {
			log.Println("error parsing authorized key line:", string(keyLine), "error:", err)
			continue
		}

		username := strings.Split(key.Comment, "@")[0]
		result[string(key.Fingerprint())] = username
	}
	return result
}
