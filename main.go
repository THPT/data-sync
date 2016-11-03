package main

import (
	"data-sync/converter"
	"fmt"
	"os"
)

func main() {
	args := os.Args[1:]
	if len(args) < 2 {
		fmt.Println("Wrong syntax")
		fmt.Println("data-sync <action> <config.json>")
		return
	}
	action := args[0]

	// Validate config file
	configFile := args[1]
	if len(configFile) < len("*.json") {
		fmt.Println("Invalid config file, must be json")
		return
	}

	switch action {
	case "import":
		converter.Import(configFile)
	case "export":
		converter.Export(configFile)
	default:
		fmt.Println("Invalid action")
		return
	}
}
