package queue

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
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
