package model

type DataPoint struct {
	Measurement       string
	TimestampUnixNano int64
	Tags              map[string]string
	Values            map[string]float64
}
