package resp

import (
	"bytes"
	"errors"
)

var (
	OKValue = ok{}

	UnknownError   = NewSimpleError(errors.New("unknown error"))
	InternalError  = NewSimpleError(errors.New("internal server error"))
	WrongtypeError = SimpleError{
		typ: ERRTYPE_WRONGTYPE,
		err: errors.New("Operation against a key holding the wrong kind of value"),
	}

	NullBulkStringValue = BulkString{null: true}
	NullArrayValue      = Array{null: true}
)

type ok struct{}

func (ok) marshalTo(buf *bytes.Buffer) { buf.WriteString("+OK\r\n") }

func (ok) Marshal() []byte { return []byte("+OK\r\n") }
