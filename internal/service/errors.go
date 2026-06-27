package service

import "errors"

// ErrTopicNotFound indicates the requested topic does not exist on the broker.
// The infrastructure layer (internal/kafka) translates client-specific errors
// into this domain error so the transport layer can map it to a 404 without
// importing the Kafka client.
var ErrTopicNotFound = errors.New("topic not found")

// ErrInvalidQuery indicates a malformed query parameter (e.g. a negative
// partition). It maps to a 400 at the transport layer.
var ErrInvalidQuery = errors.New("invalid query")
