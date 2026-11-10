package runner

import "errors"

// permanentError marks input that can never be processed (validation failure). The runner
// dead-letters the record instead of retrying it.
type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// Permanent wraps err so the runner sends the record to orders.dlq.
func Permanent(err error) error { return permanentError{err} }

// IsPermanent reports whether err (or any error it wraps) is permanent.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

// ErrInjected is the transient error produced by the processingErrorProbability knob.
var ErrInjected = errors.New("injected transient processing error")
