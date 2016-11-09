package converter

import (
	"fmt"
	"io/ioutil"
	"path/filepath"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	yaml "gopkg.in/yaml.v2"
)

const (
	filePath     = "./config/config.yaml"
	actionExport = "export"
	actionImport = "import"
)

var config configuration

type dbConfig struct {
	Type     string `yaml:"type"`
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DBName   string `yaml:"dbname"`
}

type configuration struct {
	Source      dbConfig `yaml:"source"`
	Destination dbConfig `yaml:"destination"`
}

func init() {
	var fileName string
	var yamlFile []byte
	var err error

	if fileName, err = filepath.Abs(filePath); err != nil {
		panic(err)
	}

	if yamlFile, err = ioutil.ReadFile(fileName); err != nil {
		panic(err)
	}
	config = configuration{}
	if err = yaml.Unmarshal(yamlFile, &config); err != nil {
		panic(err)
	}

}

func openConnection(action string) (*sqlx.DB, error) {
	var conf dbConfig
	switch action {
	case actionExport:
		conf = config.Source
	case actionImport:
		conf = config.Destination
	}
	connectionString := ""
	switch conf.Type {
	case "mysql":
		connectionString = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?allowAllFiles=true&charset=utf8mb4", conf.User, conf.Password, conf.Host, conf.Port, conf.DBName)
	case "postgres":
		connectionString = fmt.Sprintf("user=%s password=%s host=%s port=%s dbname=%s sslmode=disable", conf.User, conf.Password, conf.Host, conf.Port, conf.DBName)
	default:
		panic("Database type must be postgres or mysql")
	}
	return sqlx.Open(conf.Type, connectionString)
}
