package cmd

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jr-dragon/peridot/pkg/resp"
)

var (
	ErrCommand    = errors.New("cmd")
	ErrWrongArity = fmt.Errorf("%w: wrong number of arguments", ErrCommand)
)

type Error struct {
	resp.SimpleError

	cause   error
	message string
}

var (
	_ error      = (*Error)(nil)
	_ resp.Value = (*Error)(nil)
)

// NewError creates a client-visible command error with a Redis error code and
// message. It always matches ErrCommand and preserves cause for errors.Is/As.
// code must be a non-empty protocol token without whitespace.
func NewError(cause error, code, message string) *Error {
	if cause == nil {
		cause = ErrCommand
	} else if !errors.Is(cause, ErrCommand) {
		cause = fmt.Errorf("%w: %w", ErrCommand, cause)
	}
	return &Error{
		SimpleError: resp.NewSimpleErrorWithCode(code, errors.New(message)),
		cause:       cause,
		message:     message,
	}
}

// NewUnknownCommandError formats an unknown command reply like Redis 8.10.2.
func NewUnknownCommandError(args []resp.BulkString) *Error {
	const previewBytes = 128
	var command string
	if len(args) > 0 {
		command = args[0].String()
	}
	message := fmt.Sprintf("unknown command '%s'", errorArgumentPrefix(command, previewBytes))
	if len(args) > 1 {
		var preview strings.Builder
		for _, arg := range args[1:] {
			if preview.Len() >= previewBytes {
				break
			}
			// Redis budgets the argument bytes before adding quotes and a space,
			// so the final preview can exceed the budget by up to three bytes.
			prefix := errorArgumentPrefix(arg.String(), previewBytes-preview.Len())
			preview.WriteByte('\'')
			preview.WriteString(prefix)
			preview.WriteString("' ")
		}
		message += ", with args beginning with: " + preview.String()
	}
	message = strings.ReplaceAll(strings.ReplaceAll(message, "\r", " "), "\n", " ")
	return NewError(ErrCommand, resp.ERRTYPE_DEFAULT, message)
}

// errorArgumentPrefix matches Redis's C string formatting: byte precision and
// termination at the first NUL, including when the byte limit splits UTF-8.
func errorArgumentPrefix(arg string, limit int) string {
	arg = arg[:min(len(arg), limit)]
	if i := strings.IndexByte(arg, 0); i >= 0 {
		arg = arg[:i]
	}
	return arg
}

func newWrongNumberOfArgumentsError(cmd Command) *Error {
	message := fmt.Sprintf(
		"wrong number of arguments for '%s' command",
		strings.ToLower(cmd.Name()),
	)
	return NewError(ErrWrongArity, resp.ERRTYPE_DEFAULT, message)
}

func (e *Error) Error() string {
	return e.message
}

func (e *Error) Unwrap() error {
	return e.cause
}
