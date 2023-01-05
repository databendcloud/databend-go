package godatabend

import "fmt"

type QueryError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Kind    string `json:"kind"`
}

func (e *QueryError) Error() string {
	text := fmt.Sprintf("code: %d", e.Code)
	if e.Message != "" {
		text += fmt.Sprintf(", message: %s", e.Message)
	}
	if e.Kind != "" {
		text += fmt.Sprintf(", kind: %s", e.Kind)
	}
	return text
}

type DataField struct {
	Name     string     `json:"name"`
	DataType TypeDetail `json:"data_type"`
}

type TypeDetail struct {
	Type string `json:"type"`
}

type DataSchema struct {
	Fields []DataField `json:"fields"`
}

type QueryResponse struct {
	Data     [][]interface{} `json:"data"`
	Error    *QueryError     `json:"error"`
	FinalURI string          `json:"final_uri"`
	Id       string          `json:"id"`
	NextURI  string          `json:"next_uri"`
	Schema   DataSchema      `json:"schema,omitempty"`
	State    string          `json:"state"`
	Stats    QueryStats      `json:"stats"`
	StatsURI string          `json:"stats_uri"`
}

type QueryStats struct {
	RunningTimeMS float64       `json:"running_time_ms"`
	ScanProgress  QueryProgress `json:"scan_progress"`
}

type QueryProgress struct {
	Bytes uint64 `json:"bytes"`
	Rows  uint64 `json:"rows"`
}

type QueryRequest struct {
	SQL        string `json:"sql"`
	Pagination `json:"pagination"`
}

type QueryStageAttachRequest struct {
	SQL             string `json:"sql"`
	Pagination      `json:"pagination"`
	StageAttachment `json:"stage_attachment"`
}

type Pagination struct {
	WaitTime int32 `json:"wait_time_secs" default:"60"`
}

type StageAttachment struct {
	Location          string `json:"location"`
	FileFormatOptions struct {
	} `json:"file_format_options"`
	CopyOptions struct {
	} `json:"copy_options"`
}
