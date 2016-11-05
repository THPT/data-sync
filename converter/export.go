package converter

import (
	"data-sync/structure"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	delimiter = rune('	')
)

func Export(configFile string) {
	// Read table config
	file, err := ioutil.ReadFile(configFile)
	if err != nil {
		fmt.Println("Can not find config file:", err)
		return
	}

	srcDb, err := openConnection(actionExport)
	if err != nil {
		fmt.Println("Can not connect to MySQL:", err)
		panic(err)
	}
	defer srcDb.Close()
	err = srcDb.Ping()
	if err != nil {
		panic(err)
	}

	var table structure.TableStructure
	err = json.Unmarshal(file, &table)
	if err != nil {
		fmt.Println("Can not parse config file, err:", err)
		return
	}
	columns := table.Columns
	tableName := table.FromTableName

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
	query = query + selectedParams + " FROM " + tableName

	// fmt.Println("query:", query)
	tx, err := srcDb.Beginx()
	if err != nil {
		panic(err)
	}
	rows, err := tx.Queryx(query)
	if err != nil {
		fmt.Println(query)
		fmt.Println("Can not query data", err)
		tx.Rollback()
		return
	}

	// Output
	output, err := os.Create(tableName + ".tsv")
	if err != nil {
		panic(err)
	}
	defer output.Close()

	//Init writer
	tsvWriter := csv.NewWriter(output)
	tsvWriter.Comma = delimiter
	tsvWriter.UseCRLF = true

	// count := 0
	for rows.Next() {
		row, err := rows.SliceScan()
		if err != nil {
			tx.Rollback()
			panic(err)
		}

		rowStrings := make([]string, len(row))
		for i, col := range row {
			val, err := ColumnToString(col)
			if err != nil {
				tx.Rollback()
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
	tx.Commit()

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
