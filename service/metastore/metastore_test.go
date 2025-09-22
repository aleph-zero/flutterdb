package metastore

import (
    "context"
    "fmt"
    "github.com/aleph-zero/flutterdb/engine/types"
    "github.com/google/go-cmp/cmp"
    "github.com/google/go-cmp/cmp/cmpopts"
    "github.com/stretchr/testify/require"
    "io"
    "os"
    "path/filepath"
    "testing"
)

const data = "../../testdata/metastore"

// use this for testing new formats
//func Test_PersistNewFormat(t *testing.T) {
//
//    tmd := NewTableMetadata("t1", map[string]ColumnMetadata{
//        "c1": {ColumnName: "c1", ColumnType: types.KEYWORD},
//        "c2": {ColumnName: "c2", ColumnType: types.TEXT},
//        "c3": {ColumnName: "c3", ColumnType: types.NUMERIC},
//    })
//
//    fs := newFileStore("/tmp/andrew")
//    fs.Tables[tmd.TableName] = tmd
//
//    tmd2 := NewTableMetadata("t2", map[string]ColumnMetadata{
//        "c1": {ColumnName: "c1", ColumnType: types.KEYWORD},
//        "c2": {ColumnName: "c2", ColumnType: types.TEXT},
//        "c3": {ColumnName: "c3", ColumnType: types.NUMERIC},
//    })
//    fs.Tables[tmd2.TableName] = tmd2
//
//    data, err := json.MarshalIndent(fs, "", "  ")
//    if err != nil {
//        log.Fatalf("marshalling filestore %v", err)
//    }
//
//    if err = os.WriteFile("/tmp/b.json", data, 0444); err != nil {
//        log.Fatalf("persisting filestore %v", err)
//    }
//}

func TestServiceProvider_Open(t *testing.T) {
    teardown, _, meta := setupSuite(t, data)
    defer teardown(t)

    tests := []struct {
        table string
        tmd   *TableMetadata
    }{
        {
            table: "t1",
            tmd: NewTableMetadata("t1", map[string]ColumnMetadata{
                "c1": {ColumnName: "c1", ColumnType: types.KEYWORD},
                "c2": {ColumnName: "c2", ColumnType: types.TEXT},
                "c3": {ColumnName: "c3", ColumnType: types.INTEGER},
                "c4": {ColumnName: "c4", ColumnType: types.FLOAT},
                "c5": {ColumnName: "c5", ColumnType: types.GEOPOINT},
                "c6": {ColumnName: "c6", ColumnType: types.DATETIME},
            }, ""),
        },
    }

    for _, tt := range tests {
        t.Run(tt.table, func(t *testing.T) {
            tbl, err := meta.GetTable(tt.table)
            require.NoError(t, err)
            if diff := cmp.Diff(tt.tmd, tbl, cmpopts.IgnoreFields(TableMetadata{}, "Directory")); diff != "" {
                t.Errorf("table metadata does not match (-expected, +received):\n%s", diff)
            }
        })
    }
}

func TestServiceProvider_CreateAndPersist(t *testing.T) {
    teardown, dir, meta := setupSuite(t, data)
    defer teardown(t)

    table := NewTableMetadata("t3", map[string]ColumnMetadata{
        "c1": {ColumnName: "c1", ColumnType: types.KEYWORD},
        "c2": {ColumnName: "c2", ColumnType: types.TEXT},
        "c3": {ColumnName: "c3", ColumnType: types.INTEGER},
        "c4": {ColumnName: "c4", ColumnType: types.GEOPOINT},
        "c5": {ColumnName: "c3", ColumnType: types.DATETIME}}, "c1")

    err := meta.CreateTable(context.Background(), table)
    require.NoError(t, err)
    tbl, err := meta.GetTable(table.TableName)
    require.NoError(t, err)
    if diff := cmp.Diff(table, tbl, cmpopts.IgnoreFields(TableMetadata{}, "Directory")); diff != "" {
        t.Errorf("table metadata does not match (-expected, +received):\n%s", diff)
    }

    err = meta.Persist()
    require.NoError(t, err)
    // read newly persisted metastore into a new service
    meta2 := NewService(dir)
    err = meta2.Open()
    require.NoError(t, err)
    tbl2, err := meta2.GetTable(table.TableName)
    require.NoError(t, err)

    if diff := cmp.Diff(tbl, tbl2, cmpopts.IgnoreFields(TableMetadata{}, "Directory")); diff != "" {
        t.Errorf("table metadata does not match (-expected, +received):\n%s", diff)
    }
    if diff := cmp.Diff(table, tbl2, cmpopts.IgnoreFields(TableMetadata{}, "Directory")); diff != "" {
        t.Errorf("table metadata does not match (-expected, +received):\n%s", diff)
    }
}

func setupSuite(tb testing.TB, testdata string) (func(tb testing.TB), string, Service) {
    dir, err := createTempMetastore(filepath.Join(testdata, "metastore.json"))
    if err != nil {
        tb.Fatal(err)
    }

    ms := NewService(dir)
    if err := ms.Open(); err != nil {
        tb.Fatal(err)
    }
    return func(tb testing.TB) { /* no-op teardown */ }, dir, ms
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
