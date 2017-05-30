package parser

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/etl/web100"
)

var (
	TmpDir = "/mnt/tmpfs"
)

//=========================================================================
// NDT Test filename parsing related stuff.
//=========================================================================

// TODO - should this be optional?
const dateDir = `^(?P<dir>\d{4}/\d{2}/\d{2}/)?`

// TODO - use time.Parse to parse this part of the filename.
const dateField = `(?P<date>\d{8})`
const timeField = `(?P<time>[012]\d:[0-6]\d:\d{2}\.\d{1,10})`
const address = `(?P<address>.*)`
const suffix = `(?P<suffix>[a-z2].*)`

var (
	// Pattern for any valid test file name
	testFilePattern = regexp.MustCompile(
		"^" + dateDir + dateField + "T" + timeField + "Z_" + address + `\.` + suffix + "$")
	gzTestFilePattern = regexp.MustCompile(
		"^" + dateDir + dateField + "T" + timeField + "Z_" + address + `\.` + suffix + `\.gz$`)

	datePattern = regexp.MustCompile(dateField)
	timePattern = regexp.MustCompile("T" + timeField + "Z_")
	endPattern  = regexp.MustCompile(suffix + `$`)
)

// testInfo contains all the fields from a valid NDT test file name.
type testInfo struct {
	DateDir   string    // Optional leading date yyyy/mm/dd/
	Date      string    // The date field from the test file name
	Time      string    // The time field
	Address   string    // The remote address field
	Suffix    string    // The filename suffix
	Timestamp time.Time // The parsed timestamp, with microsecond resolution
}

func ParseNDTFileName(path string) (*testInfo, error) {
	fields := gzTestFilePattern.FindStringSubmatch(path)

	if fields == nil {
		// Try without trailing .gz
		fields = testFilePattern.FindStringSubmatch(path)
	}
	if fields == nil {
		if !datePattern.MatchString(path) {
			return nil, errors.New("Path should contain yyyymmddT: " + path)
		} else if !timePattern.MatchString(path) {
			return nil, errors.New("Path should contain Thh:mm:ss.ff...Z_: " + path)
		} else if !endPattern.MatchString(path) {
			return nil, errors.New("Path should end in \\.[a-z2].*: " + path)
		}
		return nil, errors.New("Invalid test path: " + path)
	}
	timestamp, err := time.Parse("20060102T15:04:05.999999999Z_", fields[2]+"T"+fields[3]+"Z_")
	if err != nil {
		log.Println(fields[2] + "T" + fields[3] + "   " + err.Error())
		return nil, errors.New("Invalid test path: " + path)
	}
	return &testInfo{fields[1], fields[2], fields[3], fields[4], fields[5], timestamp}, nil
}

//=========================================================================
// NDTParser stuff.
//=========================================================================

type fileInfoAndData struct {
	fn   string
	info testInfo
	data []byte
}

type NDTParser struct {
	inserter etl.Inserter
	// TODO(prod): eliminate need for tmpfs.
	tmpDir string

	timestamp string // The unique timestamp common across all files in current batch.
	time      time.Time

	// TODO(dev) Sometimes NDT writes multiple copies of c2s or s2c.  We need to save them
	// and use only the one identified in meta file.
	c2s *fileInfoAndData
	s2c *fileInfoAndData

	metaFile *MetaFileData
}

func NewNDTParser(ins etl.Inserter) *NDTParser {
	return &NDTParser{inserter: ins, tmpDir: TmpDir}
}

// ParseAndInsert extracts the last snaplog from the given raw snap log.
// Writes rawSnapLog to /mnt/tmpfs.
// TODO(prod): do not write to a temporary file; operate on byte array directly.
func (n *NDTParser) ParseAndInsert(taskInfo map[string]bigquery.Value, testName string, content []byte) error {
	// Scraper adds files to tar file in lexical order.  This groups together all
	// files in a single test, but the order of the files varies because of port number.
	// If c2s or s2c files precede the .meta file, we must cache them, and process
	// them only when the .meta file has been processed.
	// If we detect a new prefix before getting all three, we should log appropriate
	// information about that, and possibly place error rows in the BQ table.
	// TODO(prod) Ensure that archive files are also date sorted.
	info, err := ParseNDTFileName(testName)
	if err != nil {
		metrics.TestCount.WithLabelValues(
			n.TableName(), n.inserter.TableSuffix(),
			"unknown", "bad filename").Inc()
		// TODO - should log and count this.
		log.Println(err)
		return nil
	}

	taskFileName := taskInfo["filename"].(string)

	if info.Time != n.timestamp {
		// All files are processed ASAP.  However, if there is ONLY
		// a data file, or ONLY a meta file, we have to process the
		// test files anyway.
		n.handleAnomolies(taskFileName)

		n.timestamp = info.Time
		n.s2c = nil
		n.c2s = nil
		n.metaFile = nil
	}

	switch info.Suffix {
	case "c2s_snaplog":
		if n.c2s != nil {
			metrics.TestCount.WithLabelValues(
				n.TableName(), n.inserter.TableSuffix(),
				"c2s", "timestamp collision").Inc()
			log.Printf("Collision: %s and %s\n", n.c2s.fn, testName)
		}
		n.c2s = &fileInfoAndData{testName, *info, content}
		n.processTest(taskFileName, n.c2s, "c2s")
	case "s2c_snaplog":
		if n.s2c != nil {
			metrics.TestCount.WithLabelValues(
				n.TableName(), n.inserter.TableSuffix(),
				"s2c", "timestamp collision").Inc()
			log.Printf("Collision: %s and %s\n", n.s2c.fn, testName)
		}
		n.s2c = &fileInfoAndData{testName, *info, content}
		n.processTest(taskFileName, n.s2c, "s2c")
	case "meta":
		if n.metaFile != nil {
			metrics.TestCount.WithLabelValues(
				n.TableName(), n.inserter.TableSuffix(),
				"meta", "timestamp collision").Inc()
		}
		n.metaFile = ProcessMetaFile(
			n.TableName(), n.inserter.TableSuffix(), testName, content)
		if n.c2s != nil {
			n.processTest(taskFileName, n.c2s, "c2s")
		}
		if n.s2c != nil {
			n.processTest(taskFileName, n.s2c, "s2c")
		}
	case "c2s_ndttrace":
	case "s2c_ndttrace":
	case "cputime":
	default:
		metrics.TestCount.WithLabelValues(
			n.TableName(), n.inserter.TableSuffix(),
			"unknown", info.Suffix).Inc()
		return errors.New("Unknown test suffix: " + info.Suffix)
	}

	return nil
}

// In the case that we are missing one or more files, report and handle gracefully.
func (n *NDTParser) handleAnomolies(taskFileName string) {
	switch {
	case n.metaFile == nil:
		n.metaFile = &MetaFileData{} // Hack to allow processTest to run.
		if n.s2c != nil {
			metrics.TestCount.WithLabelValues(
				n.TableName(), n.inserter.TableSuffix(), "s2c", "no meta").Inc()
			// TODO enable this once noise is reduced.
			// log.Printf("No meta: %s %s\n", taskFileName, n.s2c.fn)
			n.processTest(taskFileName, n.s2c, "s2c")
		}
		if n.c2s != nil {
			metrics.TestCount.WithLabelValues(
				n.TableName(), n.inserter.TableSuffix(), "c2s", "no meta").Inc()
			// TODO enable this once noise is reduced.
			// log.Printf("No meta: %s %s\n", taskFileName, n.c2s.fn)
			n.processTest(taskFileName, n.c2s, "c2s")
		}
		if n.s2c == nil && n.c2s == nil {
			metrics.TestCount.WithLabelValues(
				n.TableName(), n.inserter.TableSuffix(), "test", "no meta,c2s,s2c").Inc()
		}
	// Now meta is non-nil
	case n.s2c == nil && n.c2s == nil:
		// Meta file but no test file.
		metrics.TestCount.WithLabelValues(
			n.TableName(), n.inserter.TableSuffix(), "meta", "no tests").Inc()
		log.Printf("No tests: %s %s\n", taskFileName, n.metaFile.TestName)
	// Now meta and at least one test are non-nil
	default:
		// We often only get meta + one, so no
		// need to log this.
	}
}

// processTest digests a single s2c or c2s test, and writes a row to the Inserter.
// ProcessMetaFile should already have been called and produced valid data in n.metaFile
// However, we often get s2c and c2s without corresponding meta files.  When this happens,
// we proceed with an empty metaFile.
func (n *NDTParser) processTest(taskFileName string, test *fileInfoAndData, testType string) {
	if n.metaFile == nil {
		// Defer processing until we get the meta file.
		return
	}

	// NOTE: this file size threshold and the number of simultaneous workers
	// defined in etl_worker.go must guarantee that all files written to
	// /mnt/tmpfs will fit.
	if len(test.data) > 10*1024*1024 {
		metrics.FunnyTests.WithLabelValues(
			n.TableName(), testType, ">10MB").Inc()
		metrics.TestCount.WithLabelValues(
			n.TableName(), n.inserter.TableSuffix(),
			testType, ">10MB").Inc()
		log.Printf("Ignoring oversize snaplog: %d, %s\n",
			len(test.data), test.fn)
		metrics.FileSizeHistogram.WithLabelValues(
			"huge").Observe(float64(len(test.data)))
		return
	} else {
		// Record the file size.
		metrics.FileSizeHistogram.WithLabelValues(
			"normal").Observe(float64(len(test.data)))
	}

	if len(test.data) < 16*1024 {
		// TODO - Use separate counter, since this is not unique across
		// the test.
		metrics.FunnyTests.WithLabelValues(
			n.TableName(), testType, "<16KB").Inc()
		log.Printf("Note: small rawSnapLog: %d, %s\n",
			len(test.data), test.fn)
	}
	if len(test.data) == 4096 {
		// TODO - Use separate counter, since this is not unique across
		// the test.
		metrics.FunnyTests.WithLabelValues(
			n.TableName(), testType, "4KB").Inc()
	}

	metrics.WorkerState.WithLabelValues("ndt").Inc()
	defer metrics.WorkerState.WithLabelValues("ndt").Dec()

	// TODO(prod): only do this once.
	// Parse the tcp-kis.txt web100 variable definition file.
	metrics.WorkerState.WithLabelValues("asset").Inc()
	defer metrics.WorkerState.WithLabelValues("asset").Dec()

	data, err := web100.Asset("tcp-kis.txt")
	if err != nil {
		// Asset missing from build.
		metrics.TestCount.WithLabelValues(
			n.TableName(), n.inserter.TableSuffix(),
			testType, "web100.Asset").Inc()
		log.Printf("web100.Asset error: %s processing %s from %s\n",
			err, test.fn, taskFileName)
		return
	}
	b := bytes.NewBuffer(data)

	// These unfortunately nest.
	metrics.WorkerState.WithLabelValues("parse-def").Inc()
	defer metrics.WorkerState.WithLabelValues("parse-def").Dec()
	legacyNames, err := web100.ParseWeb100Definitions(b)
	if err != nil {
		metrics.TestCount.WithLabelValues(
			n.TableName(), n.inserter.TableSuffix(),
			testType, "web100.ParseDef").Inc()
		log.Printf("web100.ParseDef error: %s processing %s from %s\n",
			err, test.fn, taskFileName)
		return
	}

	// TODO(prod): do not write to a temporary file; operate on byte array directly.
	// Write rawSnapLog to /mnt/tmpfs.
	tmpFile, err := ioutil.TempFile(n.tmpDir, "snaplog-")
	if err != nil {
		metrics.TestCount.WithLabelValues(
			n.TableName(), n.inserter.TableSuffix(),
			testType, "TmpFile").Inc()
		log.Printf("Failed to create tmpfile for: %s in %s, when processing: %s\n",
			test.fn, n.tmpDir, taskFileName)
		return
	}

	c := 0
	for count := 0; count < len(test.data); count += c {
		c, err = tmpFile.Write(test.data)
		if err != nil {
			metrics.TestCount.WithLabelValues(
				n.TableName(), n.inserter.TableSuffix(),
				testType, "tmpFile.Write").Inc()
			log.Printf("tmpFile.Write error: %s processing: %s from %s\n",
				err, test.fn, taskFileName)
			return
		}
	}

	tmpFile.Sync()
	// TODO(prod): Do we ever see remove errors?  Should log them.
	defer os.Remove(tmpFile.Name())

	// Open the file we created above.
	w, err := web100.Open(tmpFile.Name(), legacyNames)
	if err != nil {
		metrics.TestCount.WithLabelValues(
			n.TableName(), n.inserter.TableSuffix(),
			testType, "web100.Open").Inc()
		// These are mostly "could not parse /proc/web100/header",
		// with some "file read/write error C"
		log.Printf("web100.Open error: %s processing %s from %s\n",
			err, test.fn, taskFileName)
		return
	}
	defer w.Close()

	// Seek to either last snapshot, or snapshot 2100 if there are more than that.
	snap_count, ok := seek(w, n.TableName(), n.inserter.TableSuffix(),
		taskFileName, test.fn, testType)
	if !ok {
		// TODO - is there a previous snapshot we can use???
		return
	}

	n.getAndInsertValues(w, count, taskFileName, test, testType)
}

// Find the "last" web100 snapshot.
// Returns true if valid snapshot found.
func seek(w *web100.Web100, tableName string, suffix string, taskFileName string, testName string, testType string) (int, bool) {
	metrics.WorkerState.WithLabelValues("seek").Inc()
	defer metrics.WorkerState.WithLabelValues("seek").Dec()
	// Limit to parsing only up to 2100 snapshots.
	// NOTE: This is different from legacy pipeline!!
	for count := 0; count < 2100; count++ {
		err := w.Next()
		if err != nil {
			if err == io.EOF {
				// We expect EOF.
				return count, true
			} else {
				// FYI - something like 1/5000 logs typically have these errors.
				// They are things like "missing snaplog header" or "truncated".
				// They may be associated with specific bad machines.
				// We originally tried to get past the error, but this sometimes
				// results in corrupted data, so probably better to terminate
				// on this kind of error.
				metrics.TestCount.WithLabelValues(
					tableName, suffix, testType, "w.Next").Inc()
				log.Printf("w.Next error: %s processing snap %d from %s from %s\n",
					err, count, testName, taskFileName)
				return count, false
			}
		}
		// HACK - just to see how expensive the Values() call is...
		// parse every 10th snapshot.
		if count%10 == 0 {
			// Note: read and discard the values by not saving the Web100ValueMap.
			err := w.SnapshotValues(schema.Web100ValueMap{})
			if err != nil {
				// TODO - Use separate counter, since this is not unique across
				// the test.
				metrics.TestCount.WithLabelValues(
					tableName, suffix, testType, "w.Values").Inc()
			}
		}
	}
	return count, true
}

func (n *NDTParser) getAndInsertValues(w *web100.Web100, snap_count int, taskFileName string, test *fileInfoAndData, testType string) {
	// Extract the values from the last snapshot.
	metrics.WorkerState.WithLabelValues("parse").Inc()
	defer metrics.WorkerState.WithLabelValues("parse").Dec()

	snapValues := schema.Web100ValueMap{}
	err := w.SnapshotValues(snapValues)
	if err != nil {
		metrics.TestCount.WithLabelValues(
			n.TableName(), n.inserter.TableSuffix(),
			testType, "values-err").Inc()
		log.Printf("Error calling web100 Values() in test %s, when processing: %s\n%s\n",
			test.fn, taskFileName, err)
		return
	}

	// TODO(prod) Write a row with this data, even if the snapshot parsing fails?
	nestedConnSpec := schema.Web100ValueMap{}
	w.ConnectionSpec(nestedConnSpec)

	results := schema.NewWeb100MinimalRecord(
		w.LogVersion(), w.LogTime(),
		(map[string]bigquery.Value)(nestedConnSpec),
		(map[string]bigquery.Value)(snapValues))

	results["test_id"] = test.fn
	results["task_filename"] = taskFileName
	// This is the timestamp parsed from the filename.
	// TODO - if the timestamp parsing succeeded, do we need the error check?
	lt, err := test.info.Timestamp.MarshalText()
	if err != nil {
		log.Println(err)
		metrics.ErrorCount.WithLabelValues(
			n.inserter.TableBase(), "log_time marshal error").Inc()
	} else {
		results["log_time"] = string(lt)
	}
	now, err := time.Now().MarshalText()
	if err != nil {
		log.Println(err)
		metrics.ErrorCount.WithLabelValues(
			n.inserter.TableBase(), "parse_time marshal error").Inc()
	} else {
		results["parse_time"] = string(now)
	}

	connSpec := schema.EmptyConnectionSpec()

	anomolies := schema.Web100ValueMap{}
	if n.metaFile.TestName == "" {
		anomolies.SetBool("no_meta", true)
	}
	n.metaFile.PopulateConnSpec(connSpec)
	switch testType {
	case "c2s":
		connSpec.SetInt64("data_direction", CLIENT_TO_SERVER)
	case "s2c":
		connSpec.SetInt64("data_direction", SERVER_TO_CLIENT)
	default:
	}
	results["connection_spec"] = connSpec

	if snap_count != 2000 {
		anomolies.SetInt64("num_snaps", snap_count)
	}

	if len(anomolies) > 0 {
		results["anomolies"] = anomolies
	}
	fixValues(results)
	err = n.inserter.InsertRow(&bq.MapSaver{results})
	if err != nil {
		metrics.TestCount.WithLabelValues(
			n.TableName(), n.inserter.TableSuffix(),
			testType, "insert-err").Inc()
		// TODO: This is an insert error, that might be recoverable if we try again.
		log.Println("insert-err: " + err.Error())
		return
	} else {
		metrics.TestCount.WithLabelValues(
			n.TableName(), n.inserter.TableSuffix(),
			testType, "ok").Inc()
		return
	}
}

func (n *NDTParser) TableName() string {
	return n.inserter.TableBase()
}

// fixValues updates web100 log values that need post-processing fix-ups.
// TODO(dev): does this only apply to NDT or is NPAD also affected?
func fixValues(r schema.Web100ValueMap) {
	logEntry := r.GetMap([]string{"web100_log_entry"})

	// Always substitute, unless for some reason the snapshot value is missing.
	logEntry.SubstituteString(false, []string{"connection_spec", "local_ip"},
		[]string{"snap", "LocalAddress"})
	logEntry.SubstituteString(false, []string{"connection_spec", "remote_ip"},
		[]string{"snap", "RemAddress"})
	logEntry.SubstituteInt64(false, []string{"connection_spec", "local_af"},
		[]string{"snap", "LocalAddressType"})

	// Only substitute these if they are null, (because the .meta file was missing).
	r.SubstituteString(true, []string{"connection_spec", "server_ip"},
		[]string{"web100_log_entry", "connection_spec", "local_ip"})
	r.SubstituteInt64(true, []string{"connection_spec", "server_af"},
		[]string{"web100_log_entry", "connection_spec", "local_af"})
	r.SubstituteString(true, []string{"connection_spec", "client_ip"},
		[]string{"web100_log_entry", "connection_spec", "remote_ip"})
	r.SubstituteInt64(true, []string{"connection_spec", "client_af"},
		[]string{"web100_log_entry", "connection_spec", "local_af"})

	snap := logEntry.GetMap([]string{"snap"})
	start, ok := snap.GetInt64([]string{"StartTimeStamp"})
	if ok {
		start = 1000000 * start
		usec, ok := snap.GetInt64([]string{"StartTimeUsec"})
		if ok {
			start += usec
		}
		snap.SetInt64("StartTimeStamp", start)
	}

	// Fix local_af ?
	//  - web100_log_entry.connection_spec.local_af: IPv4 = 0, IPv6 = 1.
}
