package api

import (
    "bytes"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
)

type ProcessFunc func(map[string]interface{}) error

func ProcessJsonStream(r *http.Request, process ProcessFunc) error {
    defer r.Body.Close()

    // Peek at the first non-whitespace byte
    buf := new(bytes.Buffer)
    tee := io.TeeReader(r.Body, buf)

    firstByte := make([]byte, 1)
    _, err := tee.Read(firstByte)
    if err != nil {
        return fmt.Errorf("error reading first byte: %w", err)
    }

    // Reset the body to read from our buffer
    r.Body = io.NopCloser(io.MultiReader(buf, r.Body))

    switch firstByte[0] {
    case '[':
        return processJsonArray(r.Body, process)
    default:
        return processJsonObjects(r.Body, process)
    }
}

func processJsonArray(reader io.Reader, process ProcessFunc) error {
    decoder := json.NewDecoder(reader)

    tok, err := decoder.Token()
    if err != nil {
        return fmt.Errorf("error reading opening token: %w", err)
    }
    if delim, ok := tok.(json.Delim); !ok || delim != '[' {
        return fmt.Errorf("expected opening [")
    }

    for decoder.More() {
        var item map[string]interface{}
        if err := decoder.Decode(&item); err != nil {
            return fmt.Errorf("error decoding array item: %w", err)
        }

        if err := process(item); err != nil {
            return fmt.Errorf("error processing item: %w", err)
        }
    }

    tok, err = decoder.Token()
    if err != nil {
        return fmt.Errorf("error reading closing token: %w", err)
    }
    if delim, ok := tok.(json.Delim); !ok || delim != ']' {
        return fmt.Errorf("expected closing ]")
    }

    return nil
}

func processJsonObjects(reader io.Reader, process ProcessFunc) error {
    decoder := json.NewDecoder(reader)

    for {
        var item map[string]interface{}
        if err := decoder.Decode(&item); err != nil {
            if err == io.EOF {
                break
            }
            return fmt.Errorf("error decoding JSON object: %w", err)
        }

        if err := process(item); err != nil {
            return fmt.Errorf("error processing item: %w", err)
        }
    }

    return nil
}
