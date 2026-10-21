package resp

import "strconv"

func inlineSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\v' || b == '\f' || b == '\r' || b == '\n'
}

// parseInline handles Redis inline whitespace, quoted arguments and escapes.
func parseInline(header []byte) ([]BulkString, error) {
	if len(header) == 0 || header[len(header)-1] != '\n' {
		return nil, errInvalidHeader
	}
	src := header[:len(header)-1]
	if len(src) > 0 && src[len(src)-1] == '\r' {
		src = src[:len(src)-1]
	}
	var args []BulkString
	for i := 0; i < len(src); {
		if inlineSpace(src[i]) {
			i++
			continue
		}
		var word []byte
		var quote byte
		for i < len(src) {
			b := src[i]
			i++
			if quote == 0 {
				// Redis treats VT/FF as whitespace before words, but retains
				// them inside an unquoted word.
				if b == ' ' || b == '\t' || b == '\r' || b == '\n' {
					break
				}
				if b == '"' || b == '\'' {
					quote = b
					continue
				}
			} else {
				if b == quote {
					quote = 0
					if i < len(src) && !inlineSpace(src[i]) {
						return nil, errInvalidHeader
					}
					break
				}
				if b == '\\' && i < len(src) {
					if quote == '\'' {
						if src[i] == '\'' {
							b = src[i]
							i++
						}
					} else {
						b = src[i]
						i++
						if b == 'x' && i+2 <= len(src) {
							if n, err := strconv.ParseUint(string(src[i:i+2]), 16, 8); err == nil {
								word = append(word, byte(n))
								i += 2
								continue
							}
						}
						switch b {
						case 'n':
							b = '\n'
						case 'r':
							b = '\r'
						case 't':
							b = '\t'
						case 'b':
							b = '\b'
						case 'a':
							b = '\a'
						}
					}
				}
			}
			word = append(word, b)
		}
		if quote != 0 {
			return nil, errInvalidHeader
		}
		args = append(args, NewBulkString(string(word)))
	}
	return args, nil
}
