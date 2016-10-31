package converter

import (
	"data-sync/structure"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
)

func Import(tableName string) {
	// Read table config
	file, err := ioutil.ReadFile(fmt.Sprintf("./config/tables/%s.json", tableName))
	if err != nil {
		fmt.Println("Can not find table config")
		return
	}

	desDb, err := openConnection(actionImport)
	if err != nil {
		panic(err)
	}
	defer desDb.Close()
	err = desDb.Ping()
	if err != nil {
		panic(err)
	}

	var columns []structure.ColumnStructure
	json.Unmarshal(file, &columns)

	// Create table
	columnStrs := make([]string, len(columns))
	for i, col := range columns {
		columnStrs[i] = fmt.Sprintf(`"%s" %s`, col.ColumnName, col.DataType)
	}
	queryCreateTable := fmt.Sprintf(`CREATE TABLE "%s" ( %s );`, tableName, strings.Join(columnStrs, ", "))
	fmt.Println(queryCreateTable)
	tx, err := desDb.Beginx()
	if err != nil {
		tx.Rollback()
		panic(err)
	}
	res, err := tx.Exec(queryCreateTable)
	if err != nil {
		fmt.Println(res)
		tx.Rollback()
		panic(err)
	}

	queryCopy := fmt.Sprintf(`COPY "%s" FROM '%s' DELIMITER '	' CSV;`, tableName, "/Users/mac/projects/go/src/data-sync/output.tsv")
	fmt.Println(queryCopy)
	_, err = tx.Exec(queryCopy)
	if err != nil {
		tx.Rollback()
		panic(err)
	}

	tx.Commit()
}
