package common

type MetricQuery struct {
	ColumnList []string
	Data       [][]string
}

type MetricData struct {
	ColumnList   []string
	ColumnLength int
	Data         [][]string
}
