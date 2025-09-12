package parser

import (
	"github.com/aleph-zero/flutterdb/engine/token"
	"testing"
)

func TestScan(t *testing.T) {

	tests := []struct {
		text     string
		expected []token.TokenType
	}{
		{`a`, []token.TokenType{token.IDENTIFIER, token.EOF}},
		{`"a"`, []token.TokenType{token.STRING, token.EOF}},
		{`'a'`, []token.TokenType{token.STRING, token.EOF}},
	}

	for _, tt := range tests {
		t.Run(tt.text, func(t *testing.T) {
			tokens, err := LexicalScan(tt.text)
			if err != nil {
				t.Errorf("Scan() error = %v", err)
				return
			}

			if len(tokens) != len(tt.expected) {
				t.Errorf("expected %d tokens, received %d", len(tt.expected), len(tokens))
			}

			for i, tok := range tokens {
				if tok.TokenType != tt.expected[i] {
					t.Errorf("expected token type %s, received token type %s", tt.expected[i], tok.TokenType)
				}
			}
		})
	}
}
