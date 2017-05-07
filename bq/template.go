package bq

// This file contains table manipulations that use the low level bigquery library
// directly.

import (
	"fmt"
	"net/http"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/bigquery/v2" // For template table creation.
)

var client *http.Client

func init() {
	// Use a short timeout, so we get an error quickly if there is a problem.
	ctx, _ := context.WithTimeout(context.Background(), 60*time.Second)
	var err error
	client, err = google.DefaultClient(ctx, bigquery.BigqueryInsertdataScope)
	if err != nil {
		panic(err)
	}
}

func CreateTable(project string, dataset string, base string, suffix string) error {
	// Create new service
	s, err := bigquery.New(client)
	if err != nil {
		return err
	}

	// Create Tabledata service.
	tds := bigquery.NewTabledataService(s)

	// Create a dummy row, because it doesn't seem to do anything otherwise.
	var rows []*bigquery.TableDataInsertAllRequestRows
	row := &bigquery.TableDataInsertAllRequestRows{Json: make(map[string]bigquery.JsonValue)}
	row.Json["Name"] = "foobar"
	rows = append(rows, row)
	request := bigquery.TableDataInsertAllRequest{Rows: rows}
	request.TemplateSuffix = suffix

	call := tds.InsertAll(project, dataset, base, &request)
	resp, err := call.Do()
	fmt.Println(resp)
	return err
}
