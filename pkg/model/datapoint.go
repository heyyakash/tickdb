package model

type DataPoint struct {
	Measurement       string             `json:"measurement"`
	TimestampUnixNano int64              `json:"timestamp_unix_nano"`
	Tags              map[string]string  `json:"tags"`
	Values            map[string]float64 `json:"values"`
}
