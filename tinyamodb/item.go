package tinyamodb

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	toStrSha256 = hex.EncodeToString
)

type Item interface {
	SHA256Key() []byte
	StrSHA2526Key() string
	Value() ([]byte, error)
	Unmarshal([]byte) error
	Clone() Item
}

var (
	ErrNotFoundPartitionKey    = errors.New("not found partition key")
	ErrInvalidPartitionKeyType = errors.New("partition key must be 'string' type")
)

type tinyamodbItem struct {
	sha256Key    []byte
	strSha256Key string
	Item         map[string]types.AttributeValue
	UnixNano     int64
}

func NewTinyamoDbItem(item map[string]types.AttributeValue, c Config) (*tinyamodbItem, error) {
	var av types.AttributeValue
	for key, v := range item {
		if key == c.Table.PartitionKey {
			av = v
			break
		}
	}
	if av == nil {
		return nil, ErrNotFoundPartitionKey
	}
	avs, ok := (av).(*types.AttributeValueMemberS)
	if !ok {
		return nil, ErrInvalidPartitionKeyType
	}
	key, strKey := sum256([]byte(avs.Value))
	return &tinyamodbItem{
		sha256Key:    key,
		strSha256Key: strKey,
		Item:         item,
		UnixNano:     time.Now().UnixNano(),
	}, nil
}

func (i *tinyamodbItem) SHA256Key() []byte {
	return i.sha256Key
}
func (i *tinyamodbItem) StrSHA2526Key() string {
	return i.strSha256Key
}
func (i *tinyamodbItem) Value() ([]byte, error) {
	var buf = new(bytes.Buffer)
	var e encoder
	err := e.Encode(&types.AttributeValueMemberM{Value: i.Item}, i.UnixNano, buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
func (i *tinyamodbItem) Unmarshal(data []byte) error {
	return json.Unmarshal(data, i)
}

const (
	_bs = byte('s') // string
	_bS = byte('S') // []string
	_bn = byte('n') // number
	_bN = byte('N') // []number
	_bb = byte('b') // []byte
	_bB = byte('B') // [][]byte
	_bo = byte('o') // bool
	_bu = byte('u') // NULL
	_bl = byte('l') // list
	_bm = byte('m') // map
)

type encoder struct{}

func (e *encoder) Encode(av types.AttributeValue, unixtime int64, w io.Writer) error {
	if _, err := w.Write([]byte{byte(unixtime)}); err != nil {
		return err
	}
	return e.encode(av, w)
}
func (e *encoder) encode(av types.AttributeValue, w io.Writer) error {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return e.encodeString(v.Value, w)
	case *types.AttributeValueMemberSS:
		return e.encodeSliceS(v.Value, w)
	case *types.AttributeValueMemberN:
		return e.encodeNumber(v.Value, w)
	case *types.AttributeValueMemberNS:
		return e.encodeSliceN(v.Value, w)
	case *types.AttributeValueMemberB:
		return e.encodeBytes(v.Value, w)
	case *types.AttributeValueMemberBS:
		return e.encodeSliceB(v.Value, w)
	case *types.AttributeValueMemberBOOL:
		return e.encodeBool(v.Value, w)
	case *types.AttributeValueMemberNULL:
		return e.encodeNull(v.Value, w)
	case *types.AttributeValueMemberL:
		return e.encodeList(v.Value, w)
	case *types.AttributeValueMemberM:
		return e.encodeMap(v.Value, w)
	}
	return errors.New("unkown type")
}

func (e *encoder) encodeString(v string, w io.Writer) error {
	if _, err := w.Write([]byte{_bs, byte(len(v))}); err != nil {
		return err
	}
	_, err := w.Write([]byte(v))
	return err
}

func (e *encoder) encodeSliceS(v []string, w io.Writer) error {
	if _, err := w.Write([]byte{_bS, byte(len(v))}); err != nil {
		return err
	}
	for _, s := range v {
		if err := e.encodeString(s, w); err != nil {
			return err
		}
	}
	return nil
}

func (e *encoder) encodeNumber(v string, w io.Writer) error {
	if _, err := w.Write([]byte{_bn, byte(len(v))}); err != nil {
		return err
	}
	_, err := w.Write([]byte(v))
	return err
}

func (e *encoder) encodeSliceN(v []string, w io.Writer) error {
	if _, err := w.Write([]byte{_bN, byte(len(v))}); err != nil {
		return err
	}
	for _, n := range v {
		if err := e.encodeNumber(n, w); err != nil {
			return err
		}
	}
	return nil
}

func (e *encoder) encodeBytes(v []byte, w io.Writer) error {
	if _, err := w.Write([]byte{_bb, byte(len(v))}); err != nil {
		return err
	}
	_, err := w.Write(v)
	return err
}

func (e *encoder) encodeSliceB(v [][]byte, w io.Writer) error {
	if _, err := w.Write([]byte{_bB, byte(len(v))}); err != nil {
		return err
	}
	for _, b := range v {
		if err := e.encodeBytes(b, w); err != nil {
			return err
		}
	}
	return nil
}

func (e *encoder) encodeBool(v bool, w io.Writer) error {
	var b byte
	if v {
		b = '1'
	} else {
		b = '0'
	}
	_, err := w.Write([]byte{_bo, b})
	return err
}

func (e *encoder) encodeNull(v bool, w io.Writer) error {
	var b byte
	if v {
		b = '1'
	} else {
		b = '0'
	}
	_, err := w.Write([]byte{_bu, b})
	return err
}

func (e *encoder) encodeList(v []types.AttributeValue, w io.Writer) error {
	if _, err := w.Write([]byte{_bl, byte(len(v))}); err != nil {
		return err
	}
	for _, av := range v {
		if err := e.encode(av, w); err != nil {
			return err
		}
	}
	return nil
}

func (e *encoder) encodeMap(v map[string]types.AttributeValue, w io.Writer) error {
	if _, err := w.Write([]byte{_bm, byte(len(v))}); err != nil {
		return err
	}
	for k, av := range v {
		if err := e.encodeString(k, w); err != nil {
			return err
		}
		if err := e.encode(av, w); err != nil {
			return err
		}
	}
	return nil
}

func (i *tinyamodbItem) Clone() Item {
	return &tinyamodbItem{
		sha256Key:    i.sha256Key,
		strSha256Key: i.strSha256Key,
		Item:         i.Item,
		UnixNano:     i.UnixNano,
	}
}

type KeyTimeItem struct {
	RawKey       string    `json:"k"`
	sha256Key    []byte    `json:"-"`
	strSha256Key string    `json:"-"`
	Timestamp    time.Time `json:"-"`
	UnixNano     int64     `json:"u"`
}

func NewKeyTimeItem(rawKey string) *KeyTimeItem {
	i := &KeyTimeItem{RawKey: rawKey}
	i.init()
	return i
}

func (i *KeyTimeItem) SHA256Key() []byte {
	return i.sha256Key
}
func (i *KeyTimeItem) StrSHA2526Key() string {
	return i.strSha256Key
}

func (i *KeyTimeItem) Value() ([]byte, error) {
	return json.Marshal(*i)
}

func (i *KeyTimeItem) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, i)
	if err != nil {
		return err
	}
	i.Timestamp = time.Unix(0, i.UnixNano)
	return nil
}

func (i *KeyTimeItem) Clone() Item {
	return &KeyTimeItem{
		RawKey:       i.RawKey,
		sha256Key:    i.sha256Key,
		strSha256Key: i.strSha256Key,
		Timestamp:    i.Timestamp,
		UnixNano:     i.UnixNano,
	}
}

func (i *KeyTimeItem) init() {
	if i.Timestamp.IsZero() {
		i.Timestamp = time.Now()
	}
	if i.UnixNano == 0 {
		i.UnixNano = i.Timestamp.UnixNano()
	}
	if len(i.sha256Key) == 0 {
		h := sha256.Sum256([]byte(i.RawKey))
		i.sha256Key = h[:]
		i.strSha256Key = toStrSha256(i.sha256Key)
	}
}

func sum256(data []byte) (sha256Key []byte, strSha256Key string) {
	h := sha256.Sum256(data)
	sha256Key = h[:]
	strSha256Key = toStrSha256(sha256Key)
	return
}
