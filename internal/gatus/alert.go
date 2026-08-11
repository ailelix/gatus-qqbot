package gatus

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// Alert is the JSON contract exposed by this service. Gatus custom alerts do
// not have a built-in payload schema, so deployments must use the matching
// template documented by this project.
type Alert struct {
	State         string `json:"state"`
	EndpointName  string `json:"endpoint_name"`
	EndpointGroup string `json:"endpoint_group"`
	EndpointURL   string `json:"endpoint_url"`
	Description   string `json:"description"`
	Errors        string `json:"errors"`
}

func (a Alert) Validate() error {
	var errs []error
	if strings.TrimSpace(a.State) == "" {
		errs = append(errs, errors.New("state is required"))
	}
	if strings.TrimSpace(a.EndpointName) == "" {
		errs = append(errs, errors.New("endpoint_name is required"))
	}
	if !utf8.ValidString(a.State) || !utf8.ValidString(a.EndpointName) ||
		!utf8.ValidString(a.EndpointGroup) || !utf8.ValidString(a.EndpointURL) ||
		!utf8.ValidString(a.Description) || !utf8.ValidString(a.Errors) {
		errs = append(errs, errors.New("all fields must contain valid UTF-8"))
	}
	return errors.Join(errs...)
}

func (a Alert) LogValue() string {
	if group := strings.TrimSpace(a.EndpointGroup); group != "" {
		return group + " / " + strings.TrimSpace(a.EndpointName)
	}
	return strings.TrimSpace(a.EndpointName)
}

func Format(a Alert, prefix string, maxLength int) string {
	lines := []string{
		oneLine(a.State),
		"Endpoint: " + oneLine(a.LogValue()),
	}
	if value := oneLine(a.EndpointURL); value != "" {
		lines = append(lines, "URL: "+value)
	}
	if value := multiLine(a.Description); value != "" {
		lines = append(lines, "Description: "+value)
	}
	if value := multiLine(a.Errors); value != "" {
		lines = append(lines, "Errors: "+value)
	}
	return truncate(withPrefix(strings.Join(lines, "\n"), prefix), maxLength)
}

// FormatText prepares a preformatted Gatus text payload for QQ delivery.
func FormatText(value, prefix string, maxLength int) (string, error) {
	if !utf8.ValidString(value) {
		return "", errors.New("text body must contain valid UTF-8")
	}
	value = multiLine(value)
	if value == "" {
		return "", errors.New("text body must not be empty")
	}
	return truncate(withPrefix(value, prefix), maxLength), nil
}

// TextMetadata extracts non-sensitive log fields from the documented Gatus
// text template. Arbitrary text is ignored unless its first line is a state.
func TextMetadata(value string) (endpoint, state string) {
	lines := strings.Split(multiLine(value), "\n")
	if len(lines) == 0 {
		return "", ""
	}
	state = strings.TrimSpace(lines[0])
	if len(state) >= 2 && state[0] == '[' && state[len(state)-1] == ']' {
		state = strings.TrimSpace(state[1 : len(state)-1])
	}
	state = strings.ToUpper(state)
	if state != "TRIGGERED" && state != "RESOLVED" {
		return "", ""
	}
	for _, line := range lines[1:] {
		name, value, ok := strings.Cut(line, ":")
		if ok && strings.EqualFold(strings.TrimSpace(name), "Endpoint") {
			return oneLine(value), state
		}
	}
	return "", state
}

func withPrefix(value, prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return value
	}
	return prefix + "\n" + value
}

func oneLine(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func multiLine(value string) string {
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = strings.ReplaceAll(value, "\r", "\n")
	return strings.TrimSpace(value)
}

func truncate(value string, maxLength int) string {
	if maxLength < 1 || utf8.RuneCountInString(value) <= maxLength {
		return value
	}
	const suffix = "..."
	if maxLength <= len(suffix) {
		return suffix[:maxLength]
	}
	runes := []rune(value)
	return fmt.Sprintf("%s%s", string(runes[:maxLength-len(suffix)]), suffix)
}
