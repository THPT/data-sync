package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/jmoiron/sqlx"
)

const (
	rowPerQuery = "10"
	delimiter   = rune('	')
)

var srcDb *sqlx.DB

func initDestionation() {
	var err error
	srcDb, err = sqlx.Open("mysql", "grok:grok@/svcdb")
	if err != nil {
		fmt.Println("Can not connect to MySQL")
		panic(err)
	}
}

func export(tableName string) {
	// Read table config
	file, err := ioutil.ReadFile(fmt.Sprintf("./structure/%s.json", tableName))
	if err != nil {
		fmt.Println("Can not find table config")
		return
	}
	initDestionation()

	var columns []ColumnStructure
	json.Unmarshal(file, &columns)

	//Init writer
	tsvWriter := csv.NewWriter(os.Stdout)
	tsvWriter.Comma = delimiter

	// Buffer init
	var values = make([]interface{}, len(columns))
	for i, _ := range values {
		var ii interface{}
		values[i] = &ii
	}

	query := `SELECT `
	selectedStrs := make([]string, len(columns))
	for i, column := range columns {
		selectedStrs[i] = column.ColumnName
	}
	selectedParams := strings.Join(selectedStrs, ", ")
	query = query + selectedParams + " FROM " + tableName + " LIMIT 10"

	// fmt.Println("query:", query)
	rows, err := srcDb.Queryx(query)
	if err != nil {
		fmt.Println(query)
		fmt.Println("Can not query data", err)
		return
	}

	// count := 0
	for rows.Next() {
		row, err := rows.SliceScan()
		if err != nil {
			panic(err)
		}

		rowStrings := make([]string, len(row))
		for i, col := range row {
			val, err := ColumnToString(col)
			if err != nil {
				panic(err)
			}

			rowStrings[i] = val
		}

		tsvWriter.Write(rowStrings)
		// count++
		// if count%100 == 0 {
		// 	fmt.Println(count)
		// }
	}

	tsvWriter.Flush()
	// fmt.Printf("[FINISH] Total %d line \n", count)
}

func ColumnToString(col interface{}) (string, error) {

	switch col.(type) {
	case float64:
		return strconv.FormatFloat(col.(float64), 'f', 6, 64), nil
	case int64:
		return strconv.FormatInt(col.(int64), 10), nil
	case bool:
		return strconv.FormatBool(col.(bool)), nil
	case []byte:
		return string(col.([]byte)), nil
	case string:
		return col.(string), nil
	case time.Time:
		return col.(time.Time).String(), nil
	case nil:
		return "NULL", nil
	default:
		// Need to handle anything that ends up here
		return "", fmt.Errorf("Unknown column type %v", col)
	}
}
