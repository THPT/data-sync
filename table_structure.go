package main

type TableStructure struct {
	Columns []ColumnStructure `json:"columns"`
}

type ColumnStructure struct {
	ColumnName string `json:"column_name"`
	DataType   string `json:"data_type"`
}
