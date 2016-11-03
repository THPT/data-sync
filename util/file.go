package util

import (
	"errors"
	"os"
	"strings"
)

func GetTableName(configFile string) (string, error) {
	f, err := os.Open(configFile)
	if err != nil {
		return "", err
	}

	filePath := f.Name()
	fileName := filePath
	extensionLen := len(".json")

	if strings.Index(filePath, "/") != -1 {
		fileName = filePath[strings.LastIndex(filePath, "/")+1:]
	}

	//no path
	if len(fileName) < extensionLen || fileName[len(fileName)-extensionLen:] != ".json" {
		return "", errors.New("Invalid file")
	}

	return fileName[:len(fileName)-extensionLen], nil
}
