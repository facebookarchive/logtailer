package mongodb

import (
	"encoding/json"
	"testing"

	"github.com/facebookgo/ensure"
	"github.com/tmc/mongologtools/parser"
)

var (
	sampleUpdateLine           = `Mon Sep 22 21:35:52.398 [conn6933409] update appdata403._Installation query: { _id: "0h8XoY2Mwp", _wperm: { $in: [ "*", null ] } } update: { $set: { uniqueId: "c458c6335282784e", _updated_at: new Date(1411421752261) }, $unset: { _p_uniqueId: 1 } } nscanned:1 nupdated:1 keyUpdates:1 locks(micros) w:384 135ms`
	sampleCountLine            = `Thu Jul 10 07:20:44.797 [conn22953594] command appdata37.$cmd command: count { count: "_Installation", query: { _id: { $lt: "1" }, deviceType: { $in: [ "ios", "android" ] } }, fields: null } ntoreturn:1 keyUpdates:0 numYields: 3475 locks(micros) r:202135545 reslen:48 116584ms`
	sampleCountWithPlanSummary = `Tue Sep 15 22:12:47.551 I COMMAND  [conn78495922] command appdata425.$cmd command: count { count: "Leaderboards", query: { table: "ld", score: { $gt: 3000.0 } }, fields: {} } planSummary: COUNT_SCAN { table: 1, score: -1 } keyUpdates:0 writeConflicts:0 numYields:1 reslen:44 locks:{ Global: { acquireCount: { r: 4 } }, Database: { acquireCount: { r: 2 } }, Collection: { acquireCount: { r: 2 } } } user_key_comparison_count:2030897 block_cache_hit_count:553 block_read_count:0 block_read_byte:0 internal_key_skipped_count:221520 internal_delete_skipped_count:290609 get_from_memtable_count:0 seek_on_memtable_count:7 seek_child_seek_count:42 762ms`
	sampleEmptyCountLine       = `Thu Jul 10 06:54:27.772 [conn23086439] command data.$cmd command: count { count: "_EventDimension", query: {}, fields: null } ntoreturn:1 keyUpdates:0 locks(micros) r:158621 reslen:58 158ms`
	sampleInsertLine           = `Mon Sep 22 21:35:52.401 [conn6943826] insert appdata350._ScriptLog:INFO ninserted:1 keyUpdates:0 locks(micros) w:114787 114ms`
	sampleQueryLine            = `Mon Sep 22 21:35:02.658 [conn44275481] query appdata50.City query: { $query: { _id: { $nin: [ "w4swoNXOpD", "rnWQFxnF3Z", "ADbvzAfnLr", "kHvMcnyQot", "9iFALGKUsf", "AjwcWKxJoz", "ka4zmuedVb", "sbcbcNKUaB", "Emd4pZRLlm", "GYmAdbqMN4", "Har7wFdF8d", "MpIS9kCmlw", "Oi9beqwSfq", "Xx2EfJbquY", "vADandEYMl", "vYGZ0IEVfC", "JfULQjqfbQ", "k7OuqBjXgU", "BDsONO7dCB", "Nyo0pgG6Mg", "U0m05hvQcf", "92PeBpIKUe", "Y8BwxnVQC5", "43ddOvfeyu", "Q6nb94A8R4", "g3J3sLdQtH", "jaA3H4KFCd", "v2THvLYMBf", "y4vzd63LJM", "CUGyEc7w9q", "iyY3q9JUva", "8N0pXkDh7Y", "KcMewVKZIN", "pSjxilb3eU", "3Izfcv0Gkh", "owWqUXeCut", "1jAjkijG3h", "JWGfIkusAu", "ONbT3YcLWj", "ZSgRAxj9lJ", "fbXcWyL3eh", "jJxNZhbl3S", "mtQ0ykhMqp", "M1QDB2muML", "68FvkneVmD", "Fbc4AUrB45", "a2G8le9grl", "CCEHG6j0ge", "cLVr03agLR", "M82VOd4h5n", "1JknKBQ6g9", "QwOykLkHGy", "QmJ5NyFgPE", "ZmRDT2aiZ1", "gKUjtjfAFW", "gi7Eh3QUtE", "p0DFrXaAve", "txbklugcrr", "WMKb8tJiDz", "Qq6mtXX3Zz", "IAUUYUOS7O", "BWUQgi8gra", "Q2TrBRjzxt", "LDe7qh4Ceu", "vzbw5ozrZU", "PAjCGCKDdd", "SsixxrnCoI", "UpCsmnHT2J", "VQV5vIqgx3", "Y0MhP6RKDs", "nllIFh43YY", "o1qlyASB4z", "XSKN7zRcqj", "2bFITT0Klb", "3sbgGVBOiZ", "4E4euSg3dS", "JeEaM5j0Hg", "H8cKQAU5pB", "4xrUm4razs", "6wMyCU3Lyp", "FoeduzRktc", "X9EhKPWk9j", "ljbvbPQZWZ", "trtmraCCsv", "uyctqsQQth", "zFQSg3Syar", "q6wb3xaIGR", "TcBz4E58sX", "DHCp9YrU5y", "XIvTVvO6vr", "FD8LbQZOGY", "A3QMCQ5u2P", "9GSsU1R9ng", "68v2MCvq0l", "Fl4FnSZXDX", "Kv3DturDqk", "azuoUpI7d6", "cglL29oOXE", "jMCJVA4Lvk", "p8HpDNsjMw", "ph8wB7Hryy", "0NKwjgyZcb", "Ck6JwhLQZ9", "KBKWs8rOmf", "tuvMH390GE", "vRIRlZFPda", "n3I3deDlHP", "4oGeY77kU6", "605lZTQHAM", "G3CaNGyyew", "4WgRFODOyE", "CF4rkGJpcs", "5OgZIpE6DM" ] }, _updated_at: { $lte: new Date(1411298935000) }, $or: [ { a: { $exists: false }, b: { $lte: 8 } }, { a: { $gte: -32400, $lte: -3600 }, b: { $lte: 8 } } ], c: false, d: { $gte: 20, $lte: 26 }, _rperm: { $in: [ "*", null ] } }, $orderby: { _updated_at: -1 }, $maxScan: 500000 } cursorid:5542728988973737585 ntoreturn:3 ntoskip:0 nscanned:40229 keyUpdates:0 numYields: 29 locks(micros) r:967962 nreturned:3 reslen:221 585ms`
	sampleRemoveLine           = `Wed Dec 10 22:06:00.877 [conn18684074] remove appdata315.crud_test query: { _id: "QfnHYiOQRL", _wperm: { $in: [ null, "*" ] } } ndeleted:1 keyUpdates:0 locks(micros) w:198 0ms`
	alternateQueryLine         = `Wed Dec 10 22:18:32.425 [conn18471299] query appdata386._Join:roles:_Role query: { relatedId: "ciuxUnr9Yr" } ntoreturn:0 ntoskip:0 nscanned:0 keyUpdates:0 locks(micros) r:88 nreturned:0 reslen:20 0ms`
	sampleNonOpLine            = `Fri Oct 10 22:05:24.458 [repl writer worker 7]  appdata50.DeviceCity Btree::insert: key too large to index, skipping appdata50.DeviceCity.$data_1 45697 { : "ABCDEFGHIJKLMNOPQRSTUV..." }`
	sampleNearSphereLine       = `Thu Oct 30 00:25:47.448 [conn66678403] query appdata23.Hostel query: { $query: { location: { $nearSphere: [ 0.48651, 0.8586600065713 ], $maxDistance: 0.007848061528802385 }, _rperm: { $in: [ "*", null ] } }, $maxScan: 500000 } ntoreturn:100 ntoskip:0 nscanned:13 keyUpdates:0 locks(micros) r:957 nreturned:13 reslen:6581 0ms`
	sampleGeoBoxLine           = `Thu Oct 30 00:42:39.820 [conn66670511] query appdata43.SpeedSpot2 query: { $query: { location: { $within: { $box: [ [ 0.7727840564627, 0.69502913304249 ], [ 0.7762172836747, 0.69937692810909 ] ] } }, Venue: "Hotel", _rperm: { $in: [ "*", null ] } }, $orderby: { TestDate: -1 }, $maxScan: 500000 } ntoreturn:500 ntoskip:0 nscanned:1 scanAndOrder:1 keyUpdates:0 locks(micros) r:528 nreturned:1 reslen:963 0ms`
	sample26QueryLine          = `Wed Dec 10 23:57:46.747 [conn2] query appdata401._User query: { $maxScan: 500000.0, $query: { _id: "RTnTOIAkzx", _rperm: { $in: [ "*", null, "RTnTOIAkzx" ] } } } planSummary: IXSCAN { _id: -1 }, IXSCAN { _rperm: 1.0 } ntoreturn:0 ntoskip:0 nscanned:1 nscannedObjects:1 keyUpdates:0 numYields:0 locks(micros) r:225 nreturned:1 reslen:284 0ms`
	sample26GeoQueryLine       = `Wed Dec 10 23:57:46.747 [conn2] query appdata401._User query: { $maxScan: 500000.0, $query: { _id: "RTnTOIAkzx", _rperm: { $in: [ "*", null, "RTnTOIAkzx" ] } } } planSummary: GEO_NEAR_2D { lastUserLocation: "2d" } ntoreturn:0 ntoskip:0 nscanned:1 nscannedObjects:1 keyUpdates:0 numYields:0 locks(micros) r:225 nreturned:1 reslen:284 0ms`
	sample26UpdateLine         = `Wed Dec 10 23:57:46.747 [conn395] update test.foo query: { a: 1.0 } update: { c: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa..." } nscanned:4 nscannedObjects:4 nmoved:1 nMatched:1 nModified:1 keyUpdates:0 numYields:0 locks(micros) w:174 0ms`
	sample26CommentLine        = `Mon Jan 12 16:19:17.894 [conn1410300] query appdata9._Installation query: { $query: { channels: { $in: [ "user_hsE3uMnSLA" ] }, deviceType: { $in: [ "android", "winphone", "js" ] } }, $comment: "{app_id:19015, pushd_id:8jhKA4aaad}" } planSummary: IXSCAN { channels: 1, _created_at: 1 } ntoreturn:1854540 ntoskip:0 nscanned:1 nscannedObjects:1 keyUpdates:0 numYields:0 locks(micros) r:182 nreturned:0 reslen:20 0ms`
	sample26FAMLine            = `Thu Feb 12 21:51:29.072 [conn23218] command appdata345.$cmd command: findandmodify { findandmodify: "_Installation", query: { _id: "qaKos25LyK", _wperm: { $in: [ null, "*" ] } }, update: { $set: {_updated_at: new Date(1423777889072), country: "United States", numUses: 4 } }, new: true } update: { $set: { _updated_at: new Date(142377788907), country: "United States", numUses: 4 } } nscanned:1 nscannedObjects:1 nMatched:1 nModified:1 keyUpdates:1 numYields:0 locks(micros) w:225 reslen:704 0ms`
	sample26FAMCCLine          = `Thu Feb 12 21:51:29.072 [conn23218] command appdata345.$cmd command: findAndModify { findAndModify: "_Installation", query: { _id: "qaKos25LyK", _wperm: { $in: [ null, "*" ] } }, update: { $set: {_updated_at: new Date(1423777889072), country: "United States", numUses: 4 } }, new: true } update: { $set: { _updated_at: new Date(142377788907), country: "United States", numUses: 4 } } nscanned:1 nscannedObjects:1 nMatched:1 nModified:1 keyUpdates:1 numYields:0 locks(micros) w:225 reslen:704 0ms`
	sample26IDHackQueryLine    = `Thu Feb 12 21:51:29.072 [conn136068] query appdata368._Installation query: { _id: "w9aETYE3FO" } planSummary: IDHACK ntoreturn:0 ntoskip:0 nscanned:1 nscannedObjects:1 idhack:1 keyUpdates:0 numYields:0 locks(micros) r:33 nreturned:1 reslen:601 0ms`
	sample26CmdInsertLine      = `Tue Feb 17 21:35:02.677 [conn591462] command appdata104.$cmd command: insert { insert: "_JobStatus", documents: [ { _id: "4Ps2WjKpkX", createdAt: new Date(1424208902552), jobName: "abc123", description: "", source: "api", params: "{}", status: "pending", expiresAt: new Date(1426800902598) } ], writeConcern: { w: 1, wtimeout: 10.0 }, ordered: true } keyUpdates:0 numYields:0 locks(micros) w:144 reslen:80 0ms`
	// make sure we handle $all and $each
	sampleAllQueryLine  = `Mon Oct  5 20:53:27.002 I QUERY    [conn1098902] query appdata352.Message query: { $query: { received: { $ne: true }, b: { $ne: "20150617071237576PHM" }, _rperm: { $in: [ null, "*", "pF13eUh1pl" ] }, a: { $all: [ "1234567" ], $in: [ "1234568", "1234569", "1234570" ] } }, $maxScan: 500000, $maxTimeMS: 29000, $comment: "queryhash:d1066b6bc1549f0662fe03e0002ab6da" } planSummary: IXSCAN { a: 1, b: 1 } ntoreturn:300 ntoskip:0 nscanned:25 nscannedObjects:24 keyUpdates:0 writeConflicts:0 numYields:0 nreturned:0 reslen:20 locks:{ Global: { acquireCount: { r: 2 } }, Database: { acquireCount: { r: 1 } }, Collection: { acquireCount: { r: 1 } } } user_key_comparison_count:1959 block_cache_hit_count:28 block_read_count:0 block_read_byte:0 internal_key_skipped_count:24 internal_delete_skipped_count:0 get_from_memtable_count:24 seek_on_memtable_count:2 seek_child_seek_count:12 0ms`
	sampleEachQueryLine = `Mon Oct  5 20:21:54.712 I COMMAND  [conn888146369] command appdata495.$cmd command: findAndModify { findAndModify: "_Installation", query: { _id: "pe4lID8GWy", _wperm: { $in: [ null, "*" ] } }, update: { $set: { _updated_at: new Date(1444076514710) }, $addToSet: { channels: { $each: [ "cAPA91bGzIeB7L6HFMbyjnh-USg0Y9X53Fjgqx6lLOFlUZJuV2DtGFSCPMZ2NYQV68HU", "" ] } } }, new: true } update: { $set: { _updated_at: new Date(1444076514710) }, $addToSet: { channels: { $each: [ "cAPA91bGzIeB7L6H2XW5x8XHpRFMbyjnh-USg0Y9X53Fjgqx6lLOFlUZJuV2DtGFSCPMZ2NYQV68HU", "" ] } } } nscanned:1 nscannedObjects:1 nMatched:1 nModified:1 keyUpdates:1 writeConflicts:0 numYields:0 reslen:794 locks:{ Global: { acquireCount: { r: 2, w: 2 } }, Database: { acquireCount: { w: 2 } }, Collection: { acquireCount: { w: 1 } }, oplog: { acquireCount: { w: 1 } } } user_key_comparison_count:852 block_cache_hit_count:15 block_read_count:0 block_read_byte:0 internal_key_skipped_count:2 internal_delete_skipped_count:0 get_from_memtable_count:3 seek_on_memtable_count:3 seek_child_seek_count:15 0ms`
	// handle write ops that set ACLs
	sampleACLWriteLine = `Mon Oct  5 20:21:46.464 I COMMAND  [conn1017392768] command appdata709.$cmd command: findAndModify { findAndModify: "_Session", query: { _id: "abcdefghij" }, update: { $set: { _session_token: "r:123456", _acl: { abc123: { r: true, w: true } }, _wperm: [ "987zyx" ], createdWith: { action: "signup", authProvider: "password" }, restricted: false, _p_user: "_User$r3bC9HwcFq", _updated_at: new Date(1444076506463), _rperm: [ "987zyx" ], installationId: "44ee14bc-44ae-44d4-871e-dafc54ccdf0c", expiresAt: new Date(1475612506452) } }, new: true } update: { $set: { _session_token: "r:123456", _acl: { abc123: { r: true, w: true } }, _wperm: [ "987zyx" ], createdWith: { action: "signup", authProvider: "password" }, restricted: false, _p_user: "_User$r3bC9HwcFq", _updated_at: new Date(1444076506463), _rperm: [ "987zyx" ], installationId: "44ee14bc-44ae-44d4-871e-dafc54ccdf0c", expiresAt: new Date(1475612506452) } } nscanned:1 nscannedObjects:1 nMatched:1 nModified:1 keyUpdates:1 writeConflicts:0 numYields:0 reslen:487 locks:{ Global: { acquireCount: { r: 2, w: 2 } }, Database: { acquireCount: { w: 2 } }, Collection: { acquireCount: { w: 1 } }, oplog: { acquireCount: { w: 1 } } } user_key_comparison_count:659 block_cache_hit_count:11 block_read_count:0 block_read_byte:0 internal_key_skipped_count:0 internal_delete_skipped_count:0 get_from_memtable_count:3 seek_on_memtable_count:2 seek_child_seek_count:10 0ms`
	sample32QueryLine  = `Thu Dec 17 01:01:42.311 I QUERY    [conn43] query appdata352.HistoricPotential query: { $query: { a: "123456789", b: true, _rperm: { $in: [ null, "*", "abcdefghik" ] } }, $orderby: { _created_at: -1 }, $maxScan: 500000, $maxTimeMS: 29000, $comment: "queryhash:4dc1bff80c867af8d6a484c8d63edd9c" } planSummary: IXSCAN { a: 1, _created_at: -1 } ntoreturn:1000 ntoskip:0 keysExamined:200 docsExamined:200 cursorExhausted:1 keyUpdates:0 writeConflicts:0 numYields:0 nreturned:119 reslen:68247 locks:{ Global: { acquireCount: { r: 2 } }, Database: { acquireCount: { r: 1 } }, Collection: { acquireCount: { r: 1 } } } 2ms`
)

func TestPEGParser(t *testing.T) {
	t.Parallel()

	lines := []string{sampleUpdateLine, sampleCountLine, sampleCountWithPlanSummary, sampleEmptyCountLine, sampleInsertLine, sampleQueryLine, sampleRemoveLine, alternateQueryLine, sampleNearSphereLine, sampleGeoBoxLine, sample26QueryLine, sample26GeoQueryLine, sample26UpdateLine, sample26CommentLine, sample26FAMLine, sample26FAMCCLine, sample26IDHackQueryLine, sample26CmdInsertLine, sampleAllQueryLine, sampleEachQueryLine, sampleACLWriteLine, sample32QueryLine}

	for _, line := range lines {
		_, err := parser.ParseLogLine(line)
		if err != nil {
			t.Error(line, err)
		}
	}

	// check explicit output for one command
	result, _ := parser.ParseLogLine(sample26FAMLine)
	resultJSON, err := json.Marshal(result)
	if err != nil {
		t.Error(err)
	}
	ensure.DeepEqual(t, string(resultJSON),
		`{"command":{"findandmodify":"_Installation","new":true,"query":{"_id":"qaKos25LyK","_wperm":{"$in":[null,"*"]}},"update":{"$set":{"_updated_at":{"$date":"2015-02-12T21:51:29.072Z"},"country":"United States","numUses":4}}},"command_type":"findandmodify","context":"conn23218","duration_ms":"0","keyUpdates":1,"nMatched":1,"nModified":1,"ns":"appdata345.$cmd","nscanned":1,"nscannedObjects":1,"numYields":0,"op":"command","reslen":704,"timestamp":"Thu Feb 12 21:51:29.072","update":{"$set":{"_updated_at":{"$date":"1974-07-06T21:23:08.907Z"},"country":"United States","numUses":4}},"w":225}`)

}

func TestGetQuerySignature(t *testing.T) {
	t.Parallel()

	cases := []struct{ line, key, queryType, expected string }{
		{sampleQueryLine, "query", "query", `{"$maxScan":500000,"$orderby":{"_updated_at":-1},"$query":{"$or":[{"a":{"$exists":"?"},"b":{"$lte":"?"}},{"a":{"$gte":"?","$lte":"?"},"b":{"$lte":"?"}}],"_id":{"$nin":["?"]},"_rperm":{"$in":["?"]},"_updated_at":{"$lte":"?"},"c":"?","d":{"$gte":"?","$lte":"?"}}}`},
		{sampleUpdateLine, "query", "update", `{"_id":"?","_wperm":{"$in":["?"]}}`},
		{sample26FAMLine, "command", "command", `{"new":"?","query":{"_id":"?","_wperm":{"$in":["?"]}},"update":{"$set":{"_updated_at":"?","country":"?","numUses":"?"}}}`},
		{sample26FAMCCLine, "command", "command", `{"new":"?","query":{"_id":"?","_wperm":{"$in":["?"]}},"update":{"$set":{"_updated_at":"?","country":"?","numUses":"?"}}}`},
		{sampleCountLine, "command", "command", `{"_id":{"$lt":"?"},"deviceType":{"$in":["?"]}}`},
		{sampleEmptyCountLine, "command", "command", `{}`},
		{sampleNearSphereLine, "query", "query", `{"$maxScan":500000,"$query":{"_rperm":{"$in":["?"]},"location":{"$maxDistance":"?","$nearSphere":"[?,?]"}}}`},
		{sampleGeoBoxLine, "query", "query", `{"$maxScan":500000,"$orderby":{"TestDate":-1},"$query":{"Venue":"?","_rperm":{"$in":["?"]},"location":{"$within":{"$box":"[[?,?],[?,?]]"}}}}`},
		{alternateQueryLine, "query", "query", `{"relatedId":"?"}`},
		{sampleRemoveLine, "query", "query", `{"_id":"?","_wperm":{"$in":["?"]}}`},
		{sampleAllQueryLine, "query", "query", `{"$comment":"queryhash:d1066b6bc1549f0662fe03e0002ab6da","$maxScan":500000,"$maxTimeMS":29000,"$query":{"_rperm":{"$in":["?"]},"a":{"$all":["?"],"$in":["?"]},"b":{"$ne":"?"},"received":{"$ne":"?"}}}`},
		{sampleEachQueryLine, "command", "command", `{"new":"?","query":{"_id":"?","_wperm":{"$in":["?"]}},"update":{"$addToSet":{"channels":{"$each":["?"]}},"$set":{"_updated_at":"?"}}}`},
		{sampleACLWriteLine, "command", "command", `{"new":"?","query":{"_id":"?"},"update":{"$set":{"_acl":"?","_p_user":"?","_rperm":["?"],"_session_token":"?","_updated_at":"?","_wperm":["?"],"createdWith":{"action":"?","authProvider":"?"},"expiresAt":"?","installationId":"?","restricted":"?"}}}`},
	}

	for i, c := range cases {
		values, err := parser.ParseLogLine(c.line)
		if err != nil {
			t.Fatal(c.line, err)
		}
		queryMap := values[c.key].(map[string]interface{})
		qs, err := generateQuerySignature(queryMap, c.queryType)
		if err != nil {
			t.Fatal(err)
		}
		signature := string(qs)
		if signature != c.expected {
			t.Fatalf("case %d: Query signature did not match expected signature, got:\n'%s' but expected:\n'%s'",
				i+1, signature, c.expected)
		}
	}

}

func TestParseQueryComment(t *testing.T) {
	t.Parallel()

	// Parse the line
	values, err := parser.ParseLogLine(sample26CommentLine)
	if err != nil {
		t.Fatal(err)
	}
	// Convert the query to a map
	queryMap := values["query"].(map[string]interface{})

	_, err = generateQuerySignature(queryMap, "query")
	if err != nil {
		t.Fatal(err)
	}
	// Make sure the comment exists in the map
	if queryMap["$comment"] != "{app_id:19015, pushd_id:8jhKA4aaad}" {
		t.Fatal("Failed to parse query comment.")
	}
}

func TestParsePlanSummary(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input    string
		expected string
	}{
		{
			input:    sampleCountWithPlanSummary,
			expected: "[{\"COUNT_SCAN\":[{\"table\":1},{\"score\":-1}]}]",
		},
		{
			input:    sample26QueryLine,
			expected: "[{\"IXSCAN\":[{\"_id\":-1}]},{\"IXSCAN\":[{\"_rperm\":1}]}]",
		},
	}

	// Parse each line and check for the expected planSummary output
	for _, c := range cases {
		values, err := parser.ParseLogLine(c.input)
		ensure.Nil(t, err)
		planSummary, err := json.Marshal(values["planSummary"])
		ensure.Nil(t, err)

		ensure.DeepEqual(t, string(planSummary), c.expected)
	}
}

// Ensure we can extract the NS of several types of commands
// This is stored in a field where the key is the command type. Example:
// insert: "_JobStatus"
// findAndModify: "_Installation"
func TestParseCmdDoc(t *testing.T) {
	t.Parallel()

	values, _ := parser.ParseLogLine(sample26FAMCCLine)

	queryMap := values["command"].(map[string]interface{})
	if queryMap[values["command_type"].(string)] == nil {
		t.Fatal("Could not find key ", values["command_type"], " in command document.")
	}
	if queryMap[values["command_type"].(string)] == nil {
		t.Fatalf("Could not find key '%s' in command document.", values["command_type"])
	}

	values, _ = parser.ParseLogLine(sample26CmdInsertLine)

	queryMap = values["command"].(map[string]interface{})

	if queryMap[values["command_type"].(string)] == nil {
		t.Fatalf("Could not find key '%s' in command document.", values["command_type"])
	}

}

func TestScubaRecordPreparation(t *testing.T) {
	t.Parallel()

	profile := &MongodbProfile{}

	logtailerHost = `test-host`
	record, err := profile.ProcessRecord(sample26QueryLine)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, record.(string),
		`{"collection":"_User","context":"conn2","database":"appdata401","duration_ms":"0","global_read_lock_micros":null,"global_write_lock_micros":null,"keyUpdates":0,"logtailer_host":"test-host","nreturned":1,"ns":"appdata401._User","nscanned":1,"nscannedObjects":1,"nscanned_objects":1,"ntoreturn":0,"ntoskip":0,"numYields":0,"num_yields":0,"op":"query","parser_result":"full","planSummary":[{"IXSCAN":[{"_id":-1}]},{"IXSCAN":[{"_rperm":1}]}],"plan_summary":"[{\"IXSCAN\":[{\"_id\":-1}]},{\"IXSCAN\":[{\"_rperm\":1}]}]","query":{"$maxScan":500000,"$query":{"_id":"?","_rperm":{"$in":["?"]}}},"query_signature":"{\"$maxScan\":500000,\"$query\":{\"_id\":\"?\",\"_rperm\":{\"$in\":[\"?\"]}}}","r":225,"read_lock_micros":225,"reslen":284,"scan_and_order":null,"timestamp":"Wed Dec 10 23:57:46.747","write_conflicts":null,"write_lock_micros":null}`)
}

func Test32NscannedAlias(t *testing.T) {
	t.Parallel()

	profile := &MongodbProfile{}

	logtailerHost = `test-host`
	record, err := profile.ProcessRecord(sample32QueryLine)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, record.(string),
		`{"collection":"HistoricPotential","comment":"queryhash:4dc1bff80c867af8d6a484c8d63edd9c","component":"QUERY","context":"conn43","cursorExhausted":1,"database":"appdata352","docsExamined":200,"duration_ms":"2","global_read_lock_micros":null,"global_write_lock_micros":null,"keyUpdates":0,"keysExamined":200,"locks":{"Collection":{"acquireCount":{"r":1}},"Database":{"acquireCount":{"r":1}},"Global":{"acquireCount":{"r":2}}},"logtailer_host":"test-host","nreturned":119,"ns":"appdata352.HistoricPotential","nscanned":200,"nscanned_objects":200,"ntoreturn":1000,"ntoskip":0,"numYields":0,"num_yields":0,"op":"query","parser_result":"full","planSummary":[{"IXSCAN":[{"a":1},{"_created_at":-1}]}],"plan_summary":"[{\"IXSCAN\":[{\"a\":1},{\"_created_at\":-1}]}]","query":{"$maxScan":500000,"$maxTimeMS":29000,"$orderby":{"_created_at":-1},"$query":{"_rperm":{"$in":["?"]},"a":"?","b":"?"}},"query_signature":"{\"$maxScan\":500000,\"$maxTimeMS\":29000,\"$orderby\":{\"_created_at\":-1},\"$query\":{\"_rperm\":{\"$in\":[\"?\"]},\"a\":\"?\",\"b\":\"?\"}}","read_lock_micros":null,"reslen":68247,"scan_and_order":null,"severity":"I","timestamp":"Thu Dec 17 01:01:42.311","writeConflicts":0,"write_conflicts":0,"write_lock_micros":null}`)
}
