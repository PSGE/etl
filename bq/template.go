package bq

// This file contains table manipulations that use the low level bigquery library
// directly.

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/bigquery/v2" // For template table creation.
)

var (
	httpClientOnce sync.Once // This avoids a race on setting bqClient.
	httpClient     *http.Client
)

// Returns the Singleton bigquery client for this process.
func MustGetHTTPClient(timeout time.Duration) *http.Client {
	// We do this here, instead of in init(), because we only want to do it
	// when we actually want to access the bigquery backend.
	httpClientOnce.Do(func() {
		ctx, _ := context.WithTimeout(context.Background(), timeout)
		// Heavyweight!
		var err error
		httpClient, err = google.DefaultClient(ctx, bigquery.BigqueryInsertdataScope)
		if err != nil {
			panic(err.Error())
		}
	})
	return httpClient
}

func CreateTable(project string, dataset string, base string, suffix string) error {
	// Create new service
	s, err := bigquery.New(MustGetHTTPClient(time.Minute))
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
