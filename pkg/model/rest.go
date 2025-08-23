package model

type DataPointRequest struct {
	Point DataPoint `json:"point"`
}

type DataPointBatchRequest struct {
	Points []DataPoint `json:"points"`
}

type DataPointResponse struct {
	Accepted uint64 `json:"accepted"`
	Rejected uint64 `json:"rejected"`
	Error    string `json:"error"`
}
