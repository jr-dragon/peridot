package resp

import "bytes"

// ReplySequence preserves multiple top-level replies emitted by a single Redis
// command. Redis 8.10.2 SET GET with an invalid digest emits the old value before
// the validation error. This is not an array on the wire.
type ReplySequence []Value

func (v ReplySequence) marshalTo(buf *bytes.Buffer) {
	for _, value := range v {
		value.marshalTo(buf)
	}
}

func (v ReplySequence) Marshal() []byte {
	var buf bytes.Buffer
	v.marshalTo(&buf)
	return buf.Bytes()
}
