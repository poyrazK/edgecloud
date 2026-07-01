package domain

// ContextKey is a type for context value keys, preventing collisions
// between different packages writing to the same context.
type ContextKey string

const (
	// RequestIDKey is the context key for request trace IDs.
	// Defined here so both middleware/tracing.go (the writer) and
	// handler/httperror/httperror.go (the reader) can reference
	// it without an import cycle.
	RequestIDKey ContextKey = "request_id"
)
