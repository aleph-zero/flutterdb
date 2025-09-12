package types

import (
    "encoding/json"
    "fmt"
    "strings"
)

type Type int

const (
    KEYWORD Type = iota
    TEXT
    INTEGER
    FLOAT
    GEOPOINT
    DATETIME
)

func (t Type) String() string {
    names := [...]string{"KEYWORD", "TEXT", "INTEGER", "FLOAT", "GEOPOINT", "DATETIME"}
    if t < KEYWORD || t > DATETIME {
        return fmt.Sprintf("Type(%d)", t)
    }
    return names[t]
}

func (t Type) GoString() string {
    return "types." + t.String()
}

func New(t string) (Type, error) {
    switch strings.ToUpper(t) {
    case "KEYWORD":
        return KEYWORD, nil
    case "TEXT":
        return TEXT, nil
    case "INTEGER":
        return INTEGER, nil
    case "FLOAT":
        return FLOAT, nil
    case "GEOPOINT":
        return GEOPOINT, nil
    case "DATETIME":
        return DATETIME, nil
    default:
        return -1, fmt.Errorf("invalid type: %s", t)
    }
}

func (t Type) MarshalJSON() ([]byte, error) {
    return json.Marshal(t.String())
}

func (t *Type) UnmarshalJSON(data []byte) error {
    var s string
    if err := json.Unmarshal(data, &s); err != nil {
        return err
    }
    parsed, err := New(s)
    if err != nil {
        return err
    }
    *t = parsed
    return nil
}
