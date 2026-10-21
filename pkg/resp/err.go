package resp

import (
	"errors"
	"fmt"
	"io"
)

// ProtocolError carries a client-visible command parsing reason separately from
// the internal cause. It retains errors.Is/As classification through Unwrap.
type ProtocolError struct {
	Reason string
	cause  error
}

func (e *ProtocolError) Error() string { return "Protocol error: " + e.Reason }
func (e *ProtocolError) Unwrap() error { return e.cause }

func commandProtocolError(reason string, cause error) error {
	return &ProtocolError{Reason: reason, cause: cause}
}

var (
	ErrProtocol           = errors.New("resp:")
	errInvalidHeader      = fmt.Errorf("%w: invalid header", ErrProtocol)
	errHeaderTooLong      = fmt.Errorf("%w: header too long", errInvalidHeader)
	errInvalidBody        = fmt.Errorf("%w: invalid body", ErrProtocol)
	errNegativeLength     = fmt.Errorf("%w: negative length", ErrProtocol)
	errUnexpectedSentinel = fmt.Errorf("%w: unexpected sentinel", ErrProtocol)
	errUnexpectedEOF      = fmt.Errorf("%w: %w", ErrProtocol, io.ErrUnexpectedEOF)
)
