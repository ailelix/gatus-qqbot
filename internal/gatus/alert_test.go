package gatus

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestAlertValidate(t *testing.T) {
	valid := Alert{State: "TRIGGERED", EndpointName: "api"}
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	err := (Alert{}).Validate()
	if err == nil {
		t.Fatal("Validate() error = nil")
	}
	for _, want := range []string{"state is required", "endpoint_name is required"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Validate() error %q does not contain %q", err, want)
		}
	}
}

func TestFormatIncludesAvailableFields(t *testing.T) {
	alert := Alert{
		State:         " TRIGGERED\nnow ",
		EndpointName:  " api ",
		EndpointGroup: " production ",
		EndpointURL:   " https://example.com/health ",
		Description:   "health check failed",
		Errors:        "status was 503",
	}
	want := "[Gatus]\nTRIGGERED now\n" +
		"Endpoint: production / api\n" +
		"URL: https://example.com/health\n" +
		"Description: health check failed\n" +
		"Errors: status was 503"
	if got := Format(alert, "[Gatus]", 1800); got != want {
		t.Fatalf("Format() = %q, want %q", got, want)
	}
}

func TestFormatTruncatesOnRuneBoundary(t *testing.T) {
	alert := Alert{State: "TRIGGERED", EndpointName: strings.Repeat("界", 100)}
	got := Format(alert, "[Gatus]", 64)
	if !utf8.ValidString(got) {
		t.Fatalf("Format() returned invalid UTF-8: %q", got)
	}
	if count := utf8.RuneCountInString(got); count != 64 {
		t.Fatalf("Format() rune count = %d, want 64", count)
	}
	if !strings.HasSuffix(got, "...") {
		t.Fatalf("Format() = %q, want truncation suffix", got)
	}
}

func TestFormatTextPreservesLiteralAlertContent(t *testing.T) {
	input := "TRIGGERED\r\nDescription: quoted \"value\" at C:\\health\r\nErrors: first\nsecond"
	got, err := FormatText(input, "[Gatus]", 1800)
	if err != nil {
		t.Fatalf("FormatText() error = %v", err)
	}
	want := "[Gatus]\nTRIGGERED\nDescription: quoted \"value\" at C:\\health\nErrors: first\nsecond"
	if got != want {
		t.Fatalf("FormatText() = %q, want %q", got, want)
	}
}

func TestFormatDoesNotAddBlankLineForEmptyPrefix(t *testing.T) {
	alert := Alert{State: "TRIGGERED", EndpointName: "api"}
	want := "TRIGGERED\nEndpoint: api"
	if got := Format(alert, " \t", 1800); got != want {
		t.Fatalf("Format() = %q, want %q", got, want)
	}

	got, err := FormatText("TRIGGERED\nEndpoint: api", " \t", 1800)
	if err != nil {
		t.Fatalf("FormatText() error = %v", err)
	}
	if got != want {
		t.Fatalf("FormatText() = %q, want %q", got, want)
	}
}

func TestFormatTextValidatesAndTruncates(t *testing.T) {
	for _, value := range []string{"", " \r\n\t", string([]byte{0xff})} {
		if _, err := FormatText(value, "[Gatus]", 64); err == nil {
			t.Errorf("FormatText(%q) error = nil", value)
		}
	}
	got, err := FormatText(strings.Repeat("界", 100), "", 64)
	if err != nil {
		t.Fatalf("FormatText() error = %v", err)
	}
	if utf8.RuneCountInString(got) != 64 || !strings.HasSuffix(got, "...") {
		t.Fatalf("FormatText() = %q", got)
	}
}

func TestTextMetadataExtractsDocumentedFields(t *testing.T) {
	tests := []struct {
		name         string
		body         string
		wantEndpoint string
		wantState    string
	}{
		{
			name:         "triggered",
			body:         "TRIGGERED\nEndpoint: production / website\nDescription: failed",
			wantEndpoint: "production / website",
			wantState:    "TRIGGERED",
		},
		{
			name:         "bracketed resolved",
			body:         "[RESOLVED]\r\nEndpoint:\t production   / website ",
			wantEndpoint: "production / website",
			wantState:    "RESOLVED",
		},
		{
			name: "arbitrary text",
			body: "Endpoint: do-not-log\nDescription: arbitrary message",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			endpoint, state := TextMetadata(tt.body)
			if endpoint != tt.wantEndpoint || state != tt.wantState {
				t.Fatalf("TextMetadata() = %q, %q; want %q, %q", endpoint, state, tt.wantEndpoint, tt.wantState)
			}
		})
	}
}
