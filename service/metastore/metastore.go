package metastore

import (
    "context"
    "encoding/json"
    "fmt"
    "github.com/aleph-zero/flutterdb/engine/types"
    log "github.com/go-chi/httplog/v2"
    "os"
    "path/filepath"
    "sync"
)

type Service interface {
    Open() error
    Persist() error
    CreateTable(ctx context.Context, table *TableMetadata) error
    GetTable(name string) (*TableMetadata, error)
    GetTables() []*TableMetadata
}

type ServiceProvider struct {
    filestore *filestore
}

func NewService(directory string) Service {
    return &ServiceProvider{
        filestore: newFileStore(directory),
    }
}

type filestore struct {
    lock      sync.RWMutex
    directory string
    Tables    map[string]*TableMetadata `json:"tables"`
}

func newFileStore(directory string) *filestore {
    return &filestore{
        directory: directory,
        Tables:    make(map[string]*TableMetadata),
    }
}

func (s *ServiceProvider) Open() error {
    s.filestore.lock.RLock()
    defer s.filestore.lock.RUnlock()

    data, err := os.ReadFile(s.filestore.directory + "/metastore.json")
    if err != nil {
        return fmt.Errorf("opening filestore: %w", err)
    }

    err = json.Unmarshal(data, s.filestore)
    if err != nil {
        return fmt.Errorf("unmarshalling filestore: %w", err)
    }
    return nil
}

func (s *ServiceProvider) Persist() error {
    s.filestore.lock.Lock()
    defer s.filestore.lock.Unlock()

    data, err := json.MarshalIndent(s.filestore, "", "  ")
    if err != nil {
        return fmt.Errorf("marshalling filestore: %w", err)
    }

    if err = os.WriteFile(s.filestore.directory+"/metastore.json", data, 0444); err != nil {
        return fmt.Errorf("persisting filestore: %w", err)
    }
    return nil
}

func (s *ServiceProvider) CreateTable(ctx context.Context, table *TableMetadata) error {
    s.filestore.lock.Lock()
    defer s.filestore.lock.Unlock()

    if _, ok := s.filestore.Tables[table.TableName]; ok {
        log.LogEntry(ctx).Error("Table already exists", "table", table.TableName)
        return Error{
            ErrorCode: TableExists,
            Message:   fmt.Sprintf("table %s already exists", table.TableName),
        }
    }

    table.Directory = filepath.Join(s.filestore.directory, table.TableName)
    s.filestore.Tables[table.TableName] = table
    return nil
}

func (s *ServiceProvider) GetTable(name string) (*TableMetadata, error) {
    s.filestore.lock.RLock()
    defer s.filestore.lock.RUnlock()

    table, ok := s.filestore.Tables[name]
    if !ok {
        return nil, Error{
            ErrorCode: NoSuchTable,
            Message:   fmt.Sprintf("table %s does not exist", name),
        }
    }
    return table, nil
}

func (s *ServiceProvider) GetTables() []*TableMetadata {
    s.filestore.lock.RLock()
    defer s.filestore.lock.RUnlock()

    tables := make([]*TableMetadata, 0, len(s.filestore.Tables))
    for _, v := range s.filestore.Tables {
        tables = append(tables, v)
    }
    return tables
}

type Metastore struct {
    Tables map[string]*TableMetadata `json:"tables"`
}

func NewTableMetadata(name string, columns map[string]ColumnMetadata, partition string) *TableMetadata {
    if columns == nil {
        columns = make(map[string]ColumnMetadata)
    }
    return &TableMetadata{
        TableName: name,
        Columns:   columns,
        Partition: partition,
    }
}

type TableMetadata struct {
    TableName string                    `json:"table"`
    Columns   map[string]ColumnMetadata `json:"columns"`
    Directory string                    `json:"directory"`
    Partition string                    `json:"partition,omitempty"`
}

type ColumnMetadata struct {
    ColumnName    string                `json:"column"`
    ColumnType    types.Type            `json:"type"`
    ColumnOptions ColumnMetadataOptions `json:"options,omitempty"`
}

type ColumnMetadataOptions map[string]string

/* *** Metastore  Config *** */

type Config struct {
    Directory string
}

type Option func(*Config)

func NewConfig(options ...Option) *Config {
    cfg := &Config{}
    for _, option := range options {
        option(cfg)
    }
    return cfg
}

func WithDirectory(directory string) Option {
    return func(config *Config) {
        config.Directory = directory
    }
}

/* *** Errors *** */

type ErrorCode int

const (
    TableExists ErrorCode = iota
    NoSuchTable
)

type Error struct {
    ErrorCode ErrorCode
    Message   string
    Err       error
}

func (e Error) Error() string {
    return e.Message
}

func (e Error) Unwrap() error {
    return e.Err
}

func (e Error) Is(target error) bool {
    if other, ok := target.(Error); ok {
        ignoreErrorCode := other.ErrorCode == 0
        ignoreMessage := other.Message == ""
        matchErrorCode := other.ErrorCode == e.ErrorCode
        matchMessage := other.Message == e.Message

        return matchMessage && matchErrorCode || matchMessage && ignoreErrorCode || ignoreMessage && matchErrorCode
    }
    return false
}
