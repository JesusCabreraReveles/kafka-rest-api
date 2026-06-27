package service

// Header is a single Kafka message header. Values are raw bytes to stay
// faithful to Kafka's wire model.
type Header struct {
	Key   string
	Value []byte
}

// Message is a transport-agnostic Kafka record as understood by the use-case
// layer. Key, Value, and header values are raw bytes; encoding decisions belong
// to the caller (e.g. the HTTP layer marshaling JSON).
type Message struct {
	Key     []byte
	Value   []byte
	Headers []Header
}
