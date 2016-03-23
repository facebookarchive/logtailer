package helpers

import (
	"fmt"
	"regexp"
)

// MapRe attempts to populate a map from a re and string. If an occurance is
// found the first one will be used to populate a new map.
func MapRe(re *regexp.Regexp, line string) (map[string]string, error) {
	fields := re.SubexpNames()
	match := re.FindStringSubmatch(line)
	if match == nil {
		return nil, fmt.Errorf("regex match fail: %s", line)
	}
	if len(match) != len(fields) {
		return nil, fmt.Errorf("expected len %d, got %d", len(fields), len(match))
	}
	// construct a map of field names to values
	values := make(map[string]string, len(fields))
	for i, field := range fields {
		values[field] = string(match[i])
	}
	return values, nil
}
