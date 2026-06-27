package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/JesusCabreraReveles/kafka-rest-api/internal/service"
)

// intParam parses an integer query parameter, returning def when absent.
func intParam(q url.Values, key string, def int) (int, error) {
	v := q.Get(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%w: %q must be an integer", errBadRequest, key)
	}
	return n, nil
}

// durationParam parses a Go duration query parameter, returning def when absent.
func durationParam(q url.Values, key string, def time.Duration) (time.Duration, error) {
	v := q.Get(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%w: %q must be a duration (e.g. 5s)", errBadRequest, key)
	}
	if d < 0 {
		return 0, fmt.Errorf("%w: %q must not be negative", errBadRequest, key)
	}
	return d, nil
}

// offsetParam parses the consume offset parameter. It accepts the keywords
// "earliest" (default) and "latest", or a non-negative absolute offset.
func offsetParam(q url.Values, key string) (int64, error) {
	switch v := q.Get(key); v {
	case "", "earliest":
		return service.OffsetEarliest, nil
	case "latest":
		return service.OffsetLatest, nil
	default:
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			return 0, fmt.Errorf("%w: %q must be 'earliest', 'latest', or a non-negative integer", errBadRequest, key)
		}
		return n, nil
	}
}

// Encoding tags describing how a key or value is represented in JSON output.
const (
	encodingJSON   = "json"
	encodingUTF8   = "utf8"
	encodingString = "string"
	encodingBase64 = "base64"
)

// timeParam parses a timestamp query parameter as RFC3339 or Unix seconds.
// Returns (nil, nil) when the parameter is absent.
func timeParam(q url.Values, key string) (*time.Time, error) {
	v := q.Get(key)
	if v == "" {
		return nil, nil
	}
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return &t, nil
	}
	if secs, err := strconv.ParseInt(v, 10, 64); err == nil {
		t := time.Unix(secs, 0).UTC()
		return &t, nil
	}
	return nil, fmt.Errorf("%w: %q must be RFC3339 or Unix seconds", errBadRequest, key)
}

// keyParam returns a pointer to the "key" filter value, or nil when absent.
// An explicit empty value (?key=) is preserved as a filter for empty keys.
func keyParam(q url.Values) *string {
	if !q.Has("key") {
		return nil
	}
	v := q.Get("key")
	return &v
}

// headerFilters parses repeated "header=name:value" query parameters into a map
// of required headers (all must match).
func headerFilters(q url.Values) (map[string]string, error) {
	raw := q["header"]
	if len(raw) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(raw))
	for _, hv := range raw {
		name, value, found := strings.Cut(hv, ":")
		if !found || name == "" {
			return nil, fmt.Errorf("%w: header filter %q must be in 'name:value' form", errBadRequest, hv)
		}
		out[name] = value
	}
	return out, nil
}

// encodeValue renders raw message bytes for JSON output. Valid JSON is embedded
// verbatim; anything else is base64-encoded. The returned tag tells the client
// how to interpret the value.
func encodeValue(b []byte) (json.RawMessage, string) {
	if len(b) == 0 {
		return json.RawMessage("null"), encodingJSON
	}
	if json.Valid(b) {
		return append(json.RawMessage(nil), b...), encodingJSON
	}
	encoded, _ := json.Marshal(base64.StdEncoding.EncodeToString(b))
	return encoded, encodingBase64
}

// encodeKey renders a message key as a string. UTF-8 keys are returned as-is;
// binary keys are base64-encoded. The returned tag describes which.
func encodeKey(b []byte) (value, encoding string) {
	if len(b) == 0 {
		return "", ""
	}
	if utf8.Valid(b) {
		return string(b), encodingUTF8
	}
	return base64.StdEncoding.EncodeToString(b), encodingBase64
}
