package lazily

// lazily IPC wire codec — `msgpack`, the CROSS-LANGUAGE BINARY DEFAULT
// (#lzmsgpackseven, protocol.md § Frame codecs).
//
// protocol.md makes `msgpack` MUST-level for every binding and spells out that
// shipping *a* MessagePack codec is not implementing it: the token names ONE
// wire — the externally tagged frame (`{"Snapshot": …}`) over named-field maps
// whose keys are the `json` field names, with the same omit-when-absent rule
// for optional fields. An internally tagged envelope (`{"type": 0, "value": …}`)
// or a positional array form is a private framing that happens to use
// MessagePack; a peer that negotiated `msgpack` with lazily-go could not decode
// it.
//
// The codec is built ON the json codec rather than beside it, and that is the
// point: the two differ only in how a value tree is serialized, never in the
// SHAPE of that tree. Deriving the msgpack frame from the `json` value tree
// makes the external tags, the field names, and both NodeKey rules
// (NodeSnapshot/NodeAdd omit an absent key, CrdtOp always writes it, null when
// unset) identical BY CONSTRUCTION. A second hand-written transcription of the
// same shape is exactly the drift that produced lazily-cpp's divergence.
//
// Byte payloads are ARRAYS OF INTEGERS, not MessagePack `bin`. That is what the
// reference encoder produces (`rmp_serde` serializes `Vec<u8>` through serde's
// default seq impl) and what its decoder accepts, so emitting or accepting
// `bin` would put lazily-go outside the wire it claims to speak. This is a real
// trap in Go specifically: every msgpack module encodes a `[]byte` as `bin` by
// default, so this encoder REFUSES a `[]byte` rather than guessing, and the
// decoder REFUSES `bin` in any position.
//
// NOT byte-canonical (§ Frame codecs): a MessagePack map's key order is
// encoder-defined, so conformance is `decode(encode(m)) == m` plus a decode of
// a peer's frame, never a golden byte string. This encoder happens to emit map
// keys in sorted order — permitted, but not a property any peer may rely on.

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
)

// EncodeIpcMessageMsgpack encodes an IpcMessage as a `msgpack` frame: the
// externally tagged envelope over named-field maps.
func EncodeIpcMessageMsgpack(m IpcMessage) ([]byte, error) {
	if m == nil {
		return nil, fmt.Errorf("msgpack codec: nil IpcMessage")
	}
	wire, err := m.EncodeJSON()
	if err != nil {
		return nil, fmt.Errorf("msgpack codec: %w", err)
	}
	tree, err := msgpackTreeFromJSON(wire)
	if err != nil {
		return nil, err
	}
	return msgpackEncodeValue(tree)
}

// DecodeIpcMessageMsgpack decodes a `msgpack` frame into an IpcMessage.
func DecodeIpcMessageMsgpack(data []byte) (IpcMessage, error) {
	tree, err := msgpackDecodeValue(data)
	if err != nil {
		return nil, err
	}
	wire, err := json.Marshal(tree)
	if err != nil {
		return nil, fmt.Errorf("msgpack codec: %w", err)
	}
	return IpcMessageFromWire(wire)
}

// msgpackTreeFromJSON reads the json codec's own output into the generic value
// tree the packer walks. UseNumber keeps integers exact: decoding into float64
// and re-encoding would turn a u64 node id past 2^53 into a different number.
func msgpackTreeFromJSON(wire []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(wire))
	dec.UseNumber()
	var tree any
	if err := dec.Decode(&tree); err != nil {
		return nil, fmt.Errorf("msgpack codec: re-read json tree: %w", err)
	}
	return tree, nil
}

// ---------------------------------------------------------------------------
// generic value tree -> MessagePack
// ---------------------------------------------------------------------------

func msgpackEncodeValue(v any) ([]byte, error) {
	return msgpackAppend(make([]byte, 0, 128), v)
}

func msgpackAppend(dst []byte, v any) ([]byte, error) {
	switch value := v.(type) {
	case nil:
		return append(dst, 0xc0), nil
	case bool:
		if value {
			return append(dst, 0xc3), nil
		}
		return append(dst, 0xc2), nil
	case string:
		return msgpackAppendString(dst, value), nil
	case json.Number:
		return msgpackAppendNumber(dst, value)
	case []any:
		dst = msgpackAppendArrayHeader(dst, len(value))
		for _, element := range value {
			var err error
			if dst, err = msgpackAppend(dst, element); err != nil {
				return nil, err
			}
		}
		return dst, nil
	case map[string]any:
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		dst = msgpackAppendMapHeader(dst, len(keys))
		for _, key := range keys {
			dst = msgpackAppendString(dst, key)
			var err error
			if dst, err = msgpackAppend(dst, value[key]); err != nil {
				return nil, err
			}
		}
		return dst, nil
	case []byte:
		// The `bin` trap, refused rather than guessed at. A byte payload on
		// this wire is an ARRAY OF INTEGERS; widen it before packing.
		return nil, fmt.Errorf("msgpack codec: []byte has no place on this wire — " +
			"byte payloads are arrays of integers, not MessagePack bin")
	case float64:
		return nil, fmt.Errorf("msgpack codec: frames carry no floating-point fields (got %v)", value)
	default:
		return nil, fmt.Errorf("msgpack codec: cannot encode %T", v)
	}
}

// msgpackAppendNumber writes a json.Number as a MessagePack integer. Every
// IpcMessage field is an integer, a string, or a byte sequence (§ IpcMessage),
// so a non-integral number is a frame this codec does not describe: refusing is
// better than minting a float form no peer agreed on.
func msgpackAppendNumber(dst []byte, n json.Number) ([]byte, error) {
	if i, err := strconv.ParseInt(n.String(), 10, 64); err == nil {
		return msgpackAppendInt(dst, i), nil
	}
	if u, err := strconv.ParseUint(n.String(), 10, 64); err == nil {
		return msgpackAppendUint(dst, u), nil
	}
	return nil, fmt.Errorf("msgpack codec: frames carry no floating-point fields (got %s)", n.String())
}

func msgpackAppendInt(dst []byte, i int64) []byte {
	if i >= 0 {
		return msgpackAppendUint(dst, uint64(i))
	}
	switch {
	case i >= -32:
		return append(dst, byte(0xe0|(i+32)))
	case i >= math.MinInt8:
		return append(dst, 0xd0, byte(int8(i)))
	case i >= math.MinInt16:
		dst = append(dst, 0xd1)
		return binary.BigEndian.AppendUint16(dst, uint16(int16(i)))
	case i >= math.MinInt32:
		dst = append(dst, 0xd2)
		return binary.BigEndian.AppendUint32(dst, uint32(int32(i)))
	default:
		dst = append(dst, 0xd3)
		return binary.BigEndian.AppendUint64(dst, uint64(i))
	}
}

func msgpackAppendUint(dst []byte, u uint64) []byte {
	switch {
	case u < 0x80:
		return append(dst, byte(u))
	case u <= math.MaxUint8:
		return append(dst, 0xcc, byte(u))
	case u <= math.MaxUint16:
		dst = append(dst, 0xcd)
		return binary.BigEndian.AppendUint16(dst, uint16(u))
	case u <= math.MaxUint32:
		dst = append(dst, 0xce)
		return binary.BigEndian.AppendUint32(dst, uint32(u))
	default:
		dst = append(dst, 0xcf)
		return binary.BigEndian.AppendUint64(dst, u)
	}
}

func msgpackAppendString(dst []byte, s string) []byte {
	switch n := len(s); {
	case n < 32:
		dst = append(dst, byte(0xa0|n))
	case n <= math.MaxUint8:
		dst = append(dst, 0xd9, byte(n))
	case n <= math.MaxUint16:
		dst = append(dst, 0xda)
		dst = binary.BigEndian.AppendUint16(dst, uint16(n))
	default:
		dst = append(dst, 0xdb)
		dst = binary.BigEndian.AppendUint32(dst, uint32(n))
	}
	return append(dst, s...)
}

func msgpackAppendArrayHeader(dst []byte, n int) []byte {
	switch {
	case n < 16:
		return append(dst, byte(0x90|n))
	case n <= math.MaxUint16:
		dst = append(dst, 0xdc)
		return binary.BigEndian.AppendUint16(dst, uint16(n))
	default:
		dst = append(dst, 0xdd)
		return binary.BigEndian.AppendUint32(dst, uint32(n))
	}
}

func msgpackAppendMapHeader(dst []byte, n int) []byte {
	switch {
	case n < 16:
		return append(dst, byte(0x80|n))
	case n <= math.MaxUint16:
		dst = append(dst, 0xde)
		return binary.BigEndian.AppendUint16(dst, uint16(n))
	default:
		dst = append(dst, 0xdf)
		return binary.BigEndian.AppendUint32(dst, uint32(n))
	}
}

// ---------------------------------------------------------------------------
// MessagePack -> generic value tree
// ---------------------------------------------------------------------------

// msgpackDecodeValue decodes one MessagePack value SCHEMA-LESSLY into
// nil / bool / string / json.Number / []any / map[string]any. It is both the
// first half of frame decoding and the way a test can see the ENCODING itself —
// whether a struct came out as a named-field map or as a positional array.
func msgpackDecodeValue(data []byte) (any, error) {
	r := &msgpackReader{buf: data}
	value, err := r.value()
	if err != nil {
		return nil, err
	}
	if r.pos != len(r.buf) {
		return nil, fmt.Errorf("msgpack codec: %d trailing byte(s) after the frame", len(r.buf)-r.pos)
	}
	return value, nil
}

type msgpackReader struct {
	buf []byte
	pos int
}

func (r *msgpackReader) take(n int) ([]byte, error) {
	if n < 0 || r.pos+n > len(r.buf) {
		return nil, fmt.Errorf("msgpack codec: truncated frame (want %d byte(s) at offset %d of %d)", n, r.pos, len(r.buf))
	}
	out := r.buf[r.pos : r.pos+n]
	r.pos += n
	return out, nil
}

func (r *msgpackReader) byteAt() (byte, error) {
	b, err := r.take(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func (r *msgpackReader) uintN(n int) (uint64, error) {
	b, err := r.take(n)
	if err != nil {
		return 0, err
	}
	var out uint64
	for _, v := range b {
		out = out<<8 | uint64(v)
	}
	return out, nil
}

func (r *msgpackReader) value() (any, error) {
	tag, err := r.byteAt()
	if err != nil {
		return nil, err
	}
	switch {
	case tag <= 0x7f: // positive fixint
		return msgpackUint(uint64(tag)), nil
	case tag >= 0xe0: // negative fixint
		return msgpackInt(int64(int8(tag))), nil
	case tag >= 0x80 && tag <= 0x8f:
		return r.mapBody(int(tag & 0x0f))
	case tag >= 0x90 && tag <= 0x9f:
		return r.arrayBody(int(tag & 0x0f))
	case tag >= 0xa0 && tag <= 0xbf:
		return r.stringBody(int(tag & 0x1f))
	}
	switch tag {
	case 0xc0:
		return nil, nil
	case 0xc2:
		return false, nil
	case 0xc3:
		return true, nil
	case 0xc4, 0xc5, 0xc6:
		// The `bin` family is not on this wire in ANY position. A byte payload
		// travels as an array of integers, so accepting bin here would silently
		// widen lazily-go past the wire the `msgpack` token names.
		return nil, fmt.Errorf("msgpack codec: MessagePack bin (format 0x%02x) is not on this wire — "+
			"byte payloads are arrays of integers", tag)
	case 0xca, 0xcb:
		return nil, fmt.Errorf("msgpack codec: frames carry no floating-point fields (format 0x%02x)", tag)
	case 0xcc, 0xcd, 0xce, 0xcf:
		width := 1 << (int(tag) - 0xcc)
		u, err := r.uintN(width)
		if err != nil {
			return nil, err
		}
		return msgpackUint(u), nil
	case 0xd0, 0xd1, 0xd2, 0xd3:
		width := 1 << (int(tag) - 0xd0)
		u, err := r.uintN(width)
		if err != nil {
			return nil, err
		}
		switch width {
		case 1:
			return msgpackInt(int64(int8(u))), nil
		case 2:
			return msgpackInt(int64(int16(u))), nil
		case 4:
			return msgpackInt(int64(int32(u))), nil
		default:
			return msgpackInt(int64(u)), nil
		}
	case 0xd9, 0xda, 0xdb:
		width := 1 << (int(tag) - 0xd9)
		n, err := r.uintN(width)
		if err != nil {
			return nil, err
		}
		return r.stringBody(int(n))
	case 0xdc, 0xdd:
		width := 2 << (int(tag) - 0xdc)
		n, err := r.uintN(width)
		if err != nil {
			return nil, err
		}
		return r.arrayBody(int(n))
	case 0xde, 0xdf:
		width := 2 << (int(tag) - 0xde)
		n, err := r.uintN(width)
		if err != nil {
			return nil, err
		}
		return r.mapBody(int(n))
	default:
		return nil, fmt.Errorf("msgpack codec: format 0x%02x is not on this wire", tag)
	}
}

func (r *msgpackReader) stringBody(n int) (any, error) {
	b, err := r.take(n)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

func (r *msgpackReader) arrayBody(n int) (any, error) {
	out := make([]any, 0, min(n, 1024))
	for i := 0; i < n; i++ {
		element, err := r.value()
		if err != nil {
			return nil, err
		}
		out = append(out, element)
	}
	return out, nil
}

func (r *msgpackReader) mapBody(n int) (any, error) {
	out := make(map[string]any, n)
	for i := 0; i < n; i++ {
		rawKey, err := r.value()
		if err != nil {
			return nil, err
		}
		key, ok := rawKey.(string)
		if !ok {
			return nil, fmt.Errorf("msgpack codec: struct maps are keyed by the json FIELD NAME, got a %T key", rawKey)
		}
		if _, duplicate := out[key]; duplicate {
			return nil, fmt.Errorf("msgpack codec: duplicate map key %q", key)
		}
		value, err := r.value()
		if err != nil {
			return nil, err
		}
		out[key] = value
	}
	return out, nil
}

func msgpackUint(u uint64) json.Number { return json.Number(strconv.FormatUint(u, 10)) }

func msgpackInt(i int64) json.Number { return json.Number(strconv.FormatInt(i, 10)) }
