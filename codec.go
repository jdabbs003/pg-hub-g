package pghub

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5/pgconn"
	"strconv"
	"strings"
)

func decodeKey(encoded string) ([]int64, error) {
	var value int64 = 0
	var startValue bool = true
	var foundDigit bool = false
	var sign int64 = 1
	var count uint = 1

	for i, c := range encoded {
		if c == ',' {
			if i == len(encoded)-1 {
				return nil, core.errSyntax
			}

			count++
		}
	}

	var values = make([]int64, count)
	valueNdx := 0

	for _, c := range encoded {
		if c >= '0' && c <= '9' {
			startValue = false
			foundDigit = true
			value = value*10 + int64(c-'0')
		} else if c == '-' && startValue {
			startValue = false
			sign = -1
		} else if foundDigit && (c == ',') {
			values[valueNdx] = value * sign
			valueNdx++

			value = 0
			startValue = true
			foundDigit = false
			sign = 1
		} else {
			return nil, core.errSyntax
		}
	}

	values[valueNdx] = value

	return values, nil
}

func DecodeEvent(notification *pgconn.Notification) (*Event, error) {
	payload := notification.Payload
	var key []int64
	var err1 error
	var err2 error
	var value any

	if (len(payload) == 0) || (payload[0] != 'e') {
		return nil, errors.New("invalid payload")
	}

	if len(payload) != 1 {
		sep := strings.IndexRune(payload, ':')

		if sep == -1 {
			key, err1 = decodeKey(payload[1:])
		} else {
			if sep != (len(payload) - 1) {
				buffer := bytes.NewBuffer([]byte(payload[sep+1:]))
				decoder := json.NewDecoder(buffer)
				err2 = decoder.Decode(&value)
			}

			if sep != 1 {
				key, err1 = decodeKey(payload[1:sep])
			}
		}
	}

	if (err1 != nil) || (err2 != nil) {
		return nil, errors.New("invalid payload")
	}

	return &Event{notification.Channel, key, value}, nil
}

func EncodeEvent(topic string, key []int64, value any) (string, error) {
	if !core.isValidTopic(topic) {
		return "", errors.New("invalid topic")
	}

	builder := strings.Builder{}

	builder.WriteString("notify ")
	builder.WriteString(topic)
	builder.WriteString(",$$e")

	if (key != nil) && (len(key) != 0) {
		for i, v := range key {
			if i != 0 {
				builder.WriteRune(',')
			}

			builder.WriteString(strconv.FormatInt(int64(v), 10))
		}
	}

	if value != nil {
		buffer := bytes.NewBuffer(make([]byte, 0, 8192)) //bytes.Buffer{x0}
		encoder := json.NewEncoder(buffer)
		encoder.SetEscapeHTML(false)
		encoder.SetIndent("", "")
		err := encoder.Encode(value)

		if err != nil {
			return "", errors.New("invalid value")
		}

		encodedJson := buffer.Bytes()

		if encodedJson != nil {

			// note there is an architectural defect in json.Encoder() that adds a linefeed
			// to the end of the encoded document. There's nothing you can do to turn this
			// off. So..

			if len(encodedJson) > 0 {
				encodedJson = encodedJson[:len(encodedJson)-1] // Ugh.
			}

			if len(encodedJson) > 0 {
				builder.WriteRune(':')

				for _, c := range encodedJson {

					if c == '$' {
						builder.WriteString(`\u0024`)
					} else {
						builder.WriteRune(rune(c))
					}
				}
			}
		}
	}

	builder.WriteString("$$")
	return builder.String(), nil
}
