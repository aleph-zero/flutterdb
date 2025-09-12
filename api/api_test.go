package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/aleph-zero/flutterdb/engine/types"
	"github.com/aleph-zero/flutterdb/service/metastore"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/stretchr/testify/require"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestMetastoreHandler_Create(t *testing.T) {
	server := httptest.NewServer(initializeTestRouter())
	defer server.Close()

	table := metastore.NewTableMetadata("a", map[string]metastore.ColumnMetadata{
		"a": {ColumnName: "a", ColumnType: types.KEYWORD},
		"b": {ColumnName: "b", ColumnType: types.TEXT},
		"c": {ColumnName: "c", ColumnType: types.INTEGER},
		"d": {ColumnName: "c", ColumnType: types.FLOAT},
		"e": {ColumnName: "d", ColumnType: types.GEOPOINT},
		"f": {ColumnName: "e", ColumnType: types.DATETIME}}, "")

	data, err := json.Marshal(table)
	if err != nil {
		log.Fatal(err)
	}

	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/metastore/table", server.URL), bytes.NewReader(data))
	if err != nil {
		log.Fatal(err)
	}

	res, err := server.Client().Do(req)
	if err != nil {
		log.Fatal(err)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("CREATE TABLE RESPONSE\n%s", body)
	log.Printf("Status: %d\n", res.StatusCode)
	require.Equal(t, http.StatusCreated, res.StatusCode)
}

func initializeTestRouter() chi.Router {
	router := chi.NewRouter()
	router.Use(render.SetContentType(render.ContentTypeJSON))

	dir, err := os.MkdirTemp("", "metastore")
	if err != nil {
		log.Fatal(err)
	}
	meta := metastore.NewService(dir)

	handler := NewMetastoreHandler(meta)
	router.Put("/metastore/table", handler.Create)

	return router
}
