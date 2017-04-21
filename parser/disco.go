// The parser package defines the Parser interface and implementations for the different
// test types, NDT, Paris Traceroute, and SideStream.
//
// This file defines the Parser subtype that handles DISCO data.
package parser

import (
	"bytes"
	"encoding/json"
	"log"

	"cloud.google.com/go/bigquery"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/m-lab/etl/intf"
)

//=====================================================================================
//                       Disco Parser
//=====================================================================================
type PortStats struct {
	Sample []struct { //    []Sample `json: "sample"`
		Timestamp int64   `json:"timestamp, int64"`
		Value     float32 `json:"value, float32"`
	} `json:"sample"`
	Metric     string `json:"metric"`
	Hostname   string `json:"hostname"`
	Experiment string `json:"experiment"`
	//Meta       map[string]bigquery.Value `json:"meta"`
}

// TODO(dev) add tests
type DiscoParser struct {
	inserter intf.Inserter
	// We override ParseAndInsert
	NullParser
}

func NewDiscoParser(ins intf.Inserter) intf.Parser {
	return &DiscoParser{inserter: ins}
}

// Disco data a JSON representation that should be pushed directly into BigQuery.
// For now, though, we translate it into a map, for compatibility with current inserter
// backend.
//
// Returns:
//   error on Decode error
//   error on InsertRows error
//   nil on success
//
// TODO - optimize this to use the JSON directly, if possible.
func (dp *DiscoParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, test []byte) error {
	testCount.With(prometheus.Labels{"table": dp.TableName()}).Inc()
	log.Printf("Parsing %s", testName)

	meta["testname"] = testName

	rdr := bytes.NewReader(test)
	dec := json.NewDecoder(rdr)
	for dec.More() {
		var ps PortStats
		//ps.Meta = meta
		err := dec.Decode(&ps)
		if err != nil {
			log.Printf("disco.parse %v", err)
		}
		err = dp.inserter.InsertRow(ps)
		if err != nil {
			switch t := err.(type) {
			case bigquery.PutMultiError:
				log.Printf(t[0].Error())
			default:
			}
			return err
		}
	}
	return nil
}
