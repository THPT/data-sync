package main

import (
	"io/ioutil"
	"path/filepath"

	"gopkg.in/yaml.v2"
)

var Config configuration

type dbConfig struct {
	Type     string `yaml:"type"`
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Name     string `yaml:"name"`
}

type configuration struct {
	Source      dbConfig `yaml:"source"`
	Destination dbConfig `yaml:"destination"`
}

func loads(filePath string) configuration {
	var fileName string
	var yamlFile []byte
	var err error

	if fileName, err = filepath.Abs(filePath); err != nil {
		panic(err)
	}

	if yamlFile, err = ioutil.ReadFile(fileName); err != nil {
		panic(err)
	}
	config := configuration{}
	if err = yaml.Unmarshal(yamlFile, &config); err != nil {
		panic(err)
	}

	return config
}

func main() {
	// config := loads("./config/config.yaml")
	// fmt.Printf("%+v", config)

	export("user")

}
