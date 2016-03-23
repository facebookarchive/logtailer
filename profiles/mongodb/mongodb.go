// Package mongodb implements a mongodb logtailer profile that can parse mongodb log lines
// and output JSON

package mongodb

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tmc/mongologtools/parser"

	"github.com/davecgh/go-spew/spew"
	"gopkg.in/yaml.v1"
)

var (
	enableAdditionalRocksDBFields = flag.Bool("logtailer.enablerocksdbfields", false, "Enable reporting of additional rocksdb fields.")

	outputSchema = map[string][]string{
		"int": {"ntoreturn", "idhack", "ntoskip", "nscanned", "nmoved", "scan_and_order",
			"nupdated", "fastmodinsert", "fastmod", "ninserted", "ndeleted", "keyUpdates", "num_yields",
			"global_read_lock_micros", "global_write_lock_micros", "read_lock_micros", "write_lock_micros", "nreturned",
			"reslen", "duration_ms", "sample_rate", "nscanned_objects", "nmatched", "nmodified", "upsert",
			"write_conflicts", "user_key_comparison_count", "block_cache_hit_count", "block_read_count",
			"block_read_byte", "internal_key_skipped_count", "internal_delete_skipped_count",
			"get_from_memtable_count", "seek_on_memtable_count", "seek_child_seek_count",
		},
		"normal": {"hostname", "database", "collection", "op", "query_signature",
			"command_type", "ns", "rs_mismatch", "plan_summary", "comment",
			"logtailer_host", "exception", "warning", "code", "severity",
			"component", "parser_result", "host_state",
		},
	}

	// additional rocks fields
	rocksDBFields = []string{
		"block_read_time", "block_checksum_time", "block_decompress_time", "write_wal_time", "get_snapshot_time", "get_from_memtable_time", "get_post_process_time", "get_from_output_files_time", "seek_on_memtable_time", "seek_child_seek_time", "seek_min_heap_time", "seek_internal_seek_time", "find_next_user_entry_time", "write_pre_and_post_process_time", "write_memtable_time", "db_mutex_lock_nanos", "db_condition_wait_nanos",
	}

	// maps field names to field types (int, normal, etc) populated by init().
	fieldToType map[string]string

	// holds hostname of machine running logtailer instance
	logtailerHost string
)

// MongodbProfile is the profile used to parse mongodb logs. Output is JSON
type MongodbProfile struct {
	Logger *log.Logger
}

// Init performs startup steps for the MongodbProfile
func (p *MongodbProfile) Init() error {
	p.Logger = log.New(os.Stderr, "DEBUG: ", log.LstdFlags|log.Lshortfile)

	if *enableAdditionalRocksDBFields {
		outputSchema["int"] = append(outputSchema["int"], rocksDBFields...)
	}

	fieldToType = fieldToTypeFromSchema(outputSchema)

	return nil
}

// Name returns the name of the profile and must be unique amongst registered.
// profiles
func (p *MongodbProfile) Name() string {
	return "mongodb"
}

// Convert mongo timestamp to unix UTC
func mongoTimeToUnixUTC(in string) int64 {
	// Go reference time: Mon Jan 2 15:04:05 -0700 MST 2006
	// Mongo Reference time: Thu Jul 10 06:46:11.890

	t, err := time.Parse("Mon Jan 2 15:04:05", in)
	if err != nil {
		t = time.Now().UTC()
	} else {
		// Add the current year to the end of the date string
		// This is a silly workaround for the fact that Mongo 2.4 doesn't log the current year
		// This will be fixed in 2.6
		t = t.AddDate(time.Now().UTC().Year(), 0, 0)
	}

	return t.Unix()
}

// ProcessRecord is invoked for every input log line. It returns a transformed.
// line or an error
func (p *MongodbProfile) ProcessRecord(line string) (interface{}, error) {
	values, err := parser.ParseLogLine(line)
	if err != nil {
		return nil, err
	}

	// report how well we parsed the line
	values["parser_result"] = "full"
	if _, ok := values["xextra"]; ok {
		values["parser_result"] = "partial"
		if strings.Contains(line, "......") {
			values["parser_result"] = "truncated"
		} else {
			// print parital parses to stdout for debugging purposes
			fmt.Println(line)
			spew.Dump(values)
		}
	}

	// apply transformations
	if err := p.applyTransformations(values); err != nil {
		return nil, err
	}

	var outputRecord string
	marshalled, err := json.Marshal(values)
	if err != nil {
		p.Logger.Printf("error serializing to json %s", err)
	} else {
		outputRecord = string(marshalled)
	}

	return outputRecord, nil
}

// applyTransformations takes the fields and populates more fields.
func (p *MongodbProfile) applyTransformations(match map[string]interface{}) error {
	match["logtailer_host"] = logtailerHost
	if nsParts := strings.Split(fmt.Sprint(match["ns"]), "."); len(nsParts) > 1 {
		match["database"] = nsParts[0]
		match["collection"] = nsParts[1]
	}

	// expand extra field if present:
	if asmap, ok := match["extra"]; ok {
		for k, v := range asmap.(map[string]interface{}) {
			match[k] = v
		}
	}

	// field aliases
	// in 3.2, nscanned becomes keysExamined, but we map to nscanned so we can compare
	// apples:apples
	if match["keysExamined"] != nil {
		match["nscanned"] = match["keysExamined"]
	}
	// ditto for docsExamined
	if match["docsExamined"] != nil {
		match["nscanned_objects"] = match["docsExamined"]
	}
	if match["nscannedObjects"] != nil {
		match["nscanned_objects"] = match["nscannedObjects"]
	}
	match["global_write_lock_micros"] = match["W"]
	match["global_read_lock_micros"] = match["R"]
	match["write_lock_micros"] = match["w"]
	match["read_lock_micros"] = match["r"]
	match["num_yields"] = match["numYields"]
	match["write_conflicts"] = match["writeConflicts"]
	match["scan_and_order"] = match["scanAndOrder"]

	// The collection is in the command block for commands
	// in particular we want count and and findandmodify
	if match["op"] == "command" {
		cmdMap, ok := match["command"].(map[string]interface{})
		cmdType, ctOk := match["command_type"].(string)
		if ok && ctOk {
			// If we've found a command type we're interested in, extract the value as app ID and/or
			// collection name
			if nsString, ok := cmdMap[cmdType].(string); ok {
				match["collection"] = nsString

				// Generate a query signature based on the query within the command
				if signature, err := generateQuerySignature(cmdMap, "command"); err == nil {
					match["query_signature"] = string(signature)
				} else {
					p.Logger.Printf("unable to generate command query signature. error: %s", err)
				}
			} else {
				p.Logger.Printf("unable to read the ns string from command for command type %s", cmdType)
			}
		}
	} else {
		var queryMap map[string]interface{}

		switch query := match["query"].(type) {
		case map[string]interface{}:
			queryMap = query
		case string:
			// if we didn't get a parsed query try to do our YAML/JSON dance
			queryMap, _ = loadMongoJSON(query)
		}

		if queryMap != nil {
			if comment, ok := queryMap["$comment"].(string); ok {
				// Store the comment as its own field
				match["comment"] = comment
				// Don't leave the comment in the query signature
				delete(queryMap, "$comment")
			}
			// Generate query signature if possible
			if signature, err := generateQuerySignature(queryMap, fmt.Sprint(match["op"])); err == nil {
				match["query_signature"] = string(signature)
			} else {
				p.Logger.Printf("unable to generate %s query signature. error: %s", match["op"], err)
			}
		}
	}

	if _, ok := match["planSummary"]; ok {
		if ps, err := json.Marshal(match["planSummary"]); err == nil {
			match["plan_summary"] = string(ps)
		}
	}

	return nil
}

// HandleOutput satisfies part of the profile.Profile interface, converting
// lines to JSON and printing to stdout
func (p *MongodbProfile) HandleOutput(records <-chan interface{}, dryRun bool) <-chan error {
	errChan := make(chan error)
	go func() {
		defer close(errChan)
		for record := range records {
			message, ok := record.(string)
			if !ok {
				errChan <- fmt.Errorf("Unexpected output record type: %t", record)
				continue
			}
			if dryRun {
				p.Logger.Println("skipping due to dry run")
				fmt.Println(message)
				continue
			}
			if len(message) > 0 {
				fmt.Println(message)
			}
		}
	}()
	return errChan
}

// Recurse through interface representing JSON and set values to where appropriate
func scrubFields(query interface{}) {
	switch segment := query.(type) {
	case map[string]interface{}:
		for key := range segment {
			switch key {
			case "$or", "$and":
				scrubFields(segment[key])
			case "$nearSphere":
				segment[key] = "[?,?]"
			case "$box":
				segment[key] = "[[?,?],[?,?]]"
			case "$nin", "$in", "$each", "$all":
				segment[key] = []string{"?"}
			case "_acl":
				// for legacy purposes, we still write an _acl object to some documents
				// just strip this out, it's not interesting
				segment[key] = "?"
			case "_rperm", "_wperm":
				// in insert and FAM docs, _rperm and _wperm are an array
				// in query docs, _rperm and _wperm have an $in clause
				// this handles both
				switch segment[key].(type) {
				case []interface{}:
					segment[key] = []string{"?"}
				case map[string]interface{}:
					scrubFields(segment[key])
				}
			default:
				switch segment[key].(type) {
				case map[string]interface{}:
					scrubFields(segment[key])
				case []interface{}, []string:
					scrubFields(segment[key])
				default:
					segment[key] = "?"
				}
			}
		}
	case []interface{}:
		for index := range segment {
			scrubFields(segment[index])
		}
	}
}

// package yaml generates map[interface{}]interface[] but json wants map[string][interface{}
// This doesn't really convert YAML to JSON, it just recursively changes the type
func convertYAMLToJSON(yaml interface{}) interface{} {
	switch yamlMap := yaml.(type) {
	case map[interface{}]interface{}:
		newMap := make(map[string]interface{})

		for key := range yamlMap {
			if k, ok := key.(string); ok {
				newMap[k] = convertYAMLToJSON(yamlMap[key])
			}
		}

		return newMap
	case []interface{}:
		for index := range yamlMap {
			yamlMap[index] = convertYAMLToJSON(yamlMap[index])
		}
	}

	return yaml
}

// loadMongoJSON is a special case handler for the JSON used in the slow query logs
func loadMongoJSON(json string) (map[string]interface{}, error) {
	var jsonMap interface{}

	// The queries logged in the slow query logs are not proper JSON
	// but can be parsed correctly using the yaml library
	if err := yaml.Unmarshal([]byte(json), &jsonMap); err != nil {
		return nil, err
	}

	if convertedJSON, ok := convertYAMLToJSON(jsonMap).(map[string]interface{}); ok {
		return convertedJSON, nil
	}

	return nil, fmt.Errorf("failed type assertion when loading json: '%s'", json)
}

func generateQuerySignature(queryMap map[string]interface{}, op string) ([]byte, error) {
	switch op {
	case "query":
		// For queries, scrub the $query portion of the doc if it exists
		// if it doesn't exist, scrub the whole thing
		if queryMap["$query"] == nil {
			scrubFields(queryMap)
		} else {
			scrubFields(queryMap["$query"])
		}
		return json.Marshal(queryMap)
	case "command":
		scrubFields(queryMap)
		if queryMap["count"] != nil {
			// For count only serialize the query portio
			return json.Marshal(queryMap["query"])
		}
		delete(queryMap, "findAndModify") // remove expected fields
		delete(queryMap, "findandmodify")
		return json.Marshal(queryMap)
	default:
		// For update, only the query is passed in so just return the full doc
		scrubFields(queryMap)
		return json.Marshal(queryMap)
	}
}

func fieldToTypeFromSchema(scubaSchema map[string][]string) map[string]string {
	result := make(map[string]string)
	for t, fieldList := range scubaSchema {
		for _, f := range fieldList {
			result[f] = t
		}
	}
	return result
}

func safeGetInt(v interface{}) int {
	switch value := v.(type) {
	case int:
		return value
	case int64:
		return int(value)
	case uint:
		return int(value)
	case uint64:
		return int(value)
	case string:
		i, err := strconv.Atoi(value)
		if err == nil {
			return i
		}
		return 0
	default:
		return 0
	}
}

// initalize logtailerHost
func init() {
	if hostname, err := os.Hostname(); err != nil {
		logtailerHost = "unknown"
	} else {
		logtailerHost = hostname
	}
}
