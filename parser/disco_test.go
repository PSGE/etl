package parser_test

import (
	"fmt"
	"log"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/fake"
	"github.com/m-lab/etl/intf"
	"github.com/m-lab/etl/parser"
)

// PrintingInserter prints out the items passed in.
// Inject into Parser for testing.
type PrintingInserter struct {
	bq.NullInserter
}

func (ti *PrintingInserter) InsertRow(data interface{}) error {
	fmt.Printf("%T: %v\n", data, data)
	return nil
}
func (ti *PrintingInserter) Flush() error {
	return nil
}

func TestJSONParsing(t *testing.T) {
	var test []byte = []byte(`{"sample": [{"timestamp": 69850, "value": 0.0}, {"timestamp": 69860, "value": 0.0}], "metric": "switch.multicast.local.rx", "hostname": "mlab4.sea05.measurement-lab.org", "experiment": "s1.sea05.measurement-lab.org"}
{"sample": [{"timestamp": 69850, "value": 0.0}, {"timestamp": 69860, "value": 0.0}], "metric": "switch.multicast.local.rx", "hostname": "mlab4.sea05.measurement-lab.org", "experiment": "s1.sea05.measurement-lab.org"}`)

	uploader := fake.FakeUploader{}
	// This kind of inserter, when given a struct, ...
	ins, err := bq.NewInserter(intf.InserterParams{"mlab-sandbox", "mlab_sandbox", "disco", 10 * time.Second, 1}, &uploader)

	var parser intf.Parser = parser.NewDiscoParser(ins)

	meta := make(map[string]bigquery.Value)
	err = parser.ParseAndInsert(meta, "testName", test)

	// TODO - check something

	if err != nil {
		log.Printf("%v\n", uploader.Request)
		log.Printf("%d Rows\n", len(uploader.Rows))
		log.Printf("%v\n", uploader.Rows[0])
		t.Error(err)
	}
}
