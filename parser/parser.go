// The parser package defines the Parser interface and implementations for the different
// test types, NDT, Paris Traceroute, and SideStream.
package parser

import (
	"log"

	"cloud.google.com/go/bigquery"
	"github.com/prometheus/client_golang/prometheus"
)

//=====================================================================================
//                       Parser Interface and implementations
//=====================================================================================
type Parser interface {
	// fn - Name of test file
	// table - biq query table name (for metrics and error logging only)
	// test - binary test data
	Parse(fn string, table string, test []byte) (interface{}, error)
}

//------------------------------------------------------------------------------------
type NullParser struct {
	Parser
}

func (np *NullParser) Parse(fn string, table string, test []byte) (interface{}, error) {
	testCount.With(prometheus.Labels{"table": table}).Inc()
	return nil, nil
}

//------------------------------------------------------------------------------------
// TestParser ignores the content, returns a ValueSaver with map[string]Value
// underneath, containing "filename":"..."
// TODO add tests
type TestParser struct {
	Parser
}

// TODO - use or delete this struct
type FileNameSaver struct {
	Values map[string]bigquery.Value
}

// TODO(dev) - Figure out if this can use a pointer receiver.
func (fns FileNameSaver) Save() (row map[string]bigquery.Value, insertID string, err error) {
	return fns.Values, "", nil
}

func (np *TestParser) Parse(fn string, table string, test []byte) (interface{}, error) {
	testCount.With(prometheus.Labels{"table": table}).Inc()
	log.Printf("Parsing %s", fn)
	return FileNameSaver{map[string]bigquery.Value{"filename": fn}}, nil
}

//=====================================================================================
//                       Prometheus Monitoring
//=====================================================================================

var (
	testCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "etl_parser_test_count",
		Help: "Number of tests processed.",
	}, []string{"table"})

	failureCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "etl_parser_failure_count",
		Help: "Number of test processing failures.",
	}, []string{"table", "failure_type"})
)

func init() {
	prometheus.MustRegister(testCount)
	prometheus.MustRegister(failureCount)
}
