package query

import (
	"context"
	"fmt"
	"github.com/aleph-zero/flutterdb/service/index"
	"github.com/aleph-zero/flutterdb/service/metastore"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"path/filepath"
	"testing"
)

const data = "../../testdata/metastore"

func TestServiceProvider_Execute(t *testing.T) {
	ctx := context.Background()
	teardown, service := setupSuite(t, data)
	defer teardown(t)

	tests := []struct {
		query string
	}{
		{`SELECT city, description, population FROM cities`},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			err := service.Execute(ctx, tt.query)
			require.NoError(t, err)
		})
	}
}

func setupSuite(tb testing.TB, testdata string) (func(tb testing.TB), Service) {
	dir, err := createTempMetastore(filepath.Join(testdata, "metastore.json"))
	if err != nil {
		tb.Fatal(err)
	}

	metaSvc := metastore.NewService(dir)
	if err := metaSvc.Open(); err != nil {
		tb.Fatal(err)
	}

	querySvc := NewService(metaSvc, index.NewService(metaSvc))

	return func(tb testing.TB) { /* no-op teardown */ }, querySvc
}

func createTempMetastore(srcFile string) (string, error) {
	tempDir, err := os.MkdirTemp("", "metastore-*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp directory: %w", err)
	}

	src, err := os.Open(srcFile)
	if err != nil {
		os.RemoveAll(tempDir)
		return "", fmt.Errorf("failed to open source file: %w", err)
	}
	defer src.Close()

	destPath := filepath.Join(tempDir, "metastore.json")
	dest, err := os.Create(destPath)
	if err != nil {
		os.RemoveAll(tempDir)
		return "", fmt.Errorf("failed to create destination file: %w", err)
	}
	defer dest.Close()

	_, err = io.Copy(dest, src)
	if err != nil {
		os.RemoveAll(tempDir)
		return "", fmt.Errorf("failed to copy data: %w", err)
	}

	return tempDir, nil
}
