package router

import "testing"

func TestQuoteETag(t *testing.T) {
	got := quoteETag("abc123")
	want := `"abc123"`
	if got != want {
		t.Errorf("quoteETag(%q) = %q, want %q", "abc123", got, want)
	}
}
