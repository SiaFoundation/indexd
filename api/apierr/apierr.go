// Package apierr defines errors that carry the status code the API returns
// them with.
package apierr

// A StatusError is an error the API returns with a specific status code.
type StatusError struct {
	Status  int
	Message string
}

// New returns a StatusError with the given status and message.
func New(status int, message string) *StatusError {
	return &StatusError{Status: status, Message: message}
}

// Error implements the error interface.
func (e *StatusError) Error() string {
	return e.Message
}
