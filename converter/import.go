package converter

import (
	"data-sync/structure"
	"data-sync/util"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

func Import(configFile string, rawFile string) {
	//TODO should check rawFile is valid tsv or not
	if !util.IsFileExisted(rawFile) {
		fmt.Println("Can not file raw file:", rawFile)
		return
	}

	rawPath, err := filepath.Abs(rawFile)
	if err != nil {
		panic(err)
	}

	// Read table config
	file, err := ioutil.ReadFile(configFile)
	if err != nil {
		fmt.Println("Can not find table config, err:", err)
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

	switch config.Destination.Type {
	case "postgres":
		importPostgres(desDb, file, rawPath)
	case "mysql":
		importMySQL(desDb, file, rawPath)
	default:
		fmt.Println("Invalid DB")
		return
	}

	fmt.Print("\nALL DONE\n")
}

func importMySQL(desDb *sqlx.DB, file []byte, rawPath string) {
	tx, err := desDb.Beginx()
	if err != nil {
		tx.Rollback()
		panic(err)
	}

	var table structure.TableStructure
	err = json.Unmarshal(file, &table)
	if err != nil {
		fmt.Println("Can not parse config file, err:", err)
		return
	}
	columns := table.Columns
	tableName := table.DestTableName

	// Create table
	columnStrs := make([]string, len(columns))
	for i, col := range columns {
		columnStrs[i] = fmt.Sprintf("`%s` %s", col.ColumnName, col.DataType)
	}

	newTable := tableName + "_" + fmt.Sprint(time.Now().Unix())
	queryCreateTable := fmt.Sprintf("CREATE TABLE  `%s` ( %s );", newTable, strings.Join(columnStrs, ", "))
	fmt.Println("-- Create new table")
	fmt.Println(queryCreateTable)

	res, err := tx.Exec(queryCreateTable)
	if err != nil {
		fmt.Println(res)
		tx.Rollback()
		panic(err)
	}

	stat, err := os.Stat(rawPath)
	if err == nil {
		if stat.Size() > 0 {

			fmt.Println("-- Copy data")
			//TODO should check rawFile is valid tsv or not
			queryCopy := fmt.Sprintf("LOAD DATA LOCAL INFILE '%s' INTO TABLE `%s` CHARACTER SET UTF8;", rawPath, newTable)
			fmt.Println(queryCopy)
			_, err = tx.Exec(queryCopy)
			if err != nil {
				tx.Rollback()
				panic(err)
			}
		}
	}

	//CREATE DEFAULT TABLE BEFORE
	queryDefaultTable := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`(id int)", tableName)
	fmt.Println("--Create default table")
	fmt.Println(queryDefaultTable)
	_, err = tx.Exec(queryDefaultTable)
	if err != nil {
		tx.Rollback()
		panic(err)
	}

	//Rename exist table
	queryRenameTable := fmt.Sprintf("RENAME TABLE `%s` TO `%s`", tableName, tableName+"_backup_"+fmt.Sprint(time.Now().Unix()))
	fmt.Println("-- Rename old table")
	fmt.Println(queryRenameTable)
	_, err = tx.Exec(queryRenameTable)
	if err != nil {
		tx.Rollback()
		panic(err)
	}

	// Swap to new table
	querySwapTable := fmt.Sprintf("RENAME TABLE `%s` TO `%s`", newTable, tableName)
	fmt.Println("-- Swap to new table")
	fmt.Println(querySwapTable)
	_, err = tx.Exec(querySwapTable)
	if err != nil {
		tx.Rollback()
		panic(err)
	}

	tx.Commit()
}

func importPostgres(desDb *sqlx.DB, file []byte, rawPath string) {
	tx, err := desDb.Beginx()
	if err != nil {
		tx.Rollback()
		panic(err)
	}

	var table structure.TableStructure
	err = json.Unmarshal(file, &table)
	if err != nil {
		fmt.Println("Can not parse config file, err:", err)
		return
	}
	columns := table.Columns
	tableName := table.DestTableName

	// Create table
	columnStrs := make([]string, len(columns))
	for i, col := range columns {
		columnStrs[i] = fmt.Sprintf(`"%s" %s`, col.ColumnName, col.DataType)
	}

	newTable := tableName + "_" + fmt.Sprint(time.Now().Unix())
	queryCreateTable := fmt.Sprintf(`CREATE TABLE "%s" ( %s );`, newTable, strings.Join(columnStrs, ", "))
	fmt.Println("-- Create new table")
	fmt.Println(queryCreateTable)

	res, err := tx.Exec(queryCreateTable)
	if err != nil {
		fmt.Println(res)
		tx.Rollback()
		panic(err)
	}

	fmt.Println("-- Copy data")
	//TODO should check rawFile is valid tsv or not
	queryCopy := fmt.Sprintf(`COPY "%s" FROM '%s' DELIMITER '	' CSV;`, newTable, rawPath)
	fmt.Println(queryCopy)
	_, err = tx.Exec(queryCopy)
	if err != nil {
		tx.Rollback()
		panic(err)
	}

	//Rename exist table
	queryRenameTable := fmt.Sprintf(`ALTER TABLE IF EXISTS "%s" RENAME TO "%s"`, tableName, tableName+"_backup_"+fmt.Sprint(time.Now().Unix()))
	fmt.Println("-- Rename old table")
	fmt.Println(queryRenameTable)
	_, err = tx.Exec(queryRenameTable)
	if err != nil {
		tx.Rollback()
		panic(err)
	}

	// Swap to new table
	querySwapTable := fmt.Sprintf(`ALTER TABLE "%s" RENAME TO "%s"`, newTable, tableName)
	fmt.Println("-- Swap to new table")
	fmt.Println(querySwapTable)
	_, err = tx.Exec(querySwapTable)
	if err != nil {
		tx.Rollback()
		panic(err)
	}

	tx.Commit()
}
