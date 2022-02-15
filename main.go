package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/getjump/txkv/kv"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	_kv := kv.NewKV()

	for {
		fmt.Print("> ")

		if !scanner.Scan() {
			return
		}

		text := scanner.Text()
		if len(text) == 0 || text == "exit" || text == "quit" {
			return
		}

		parts := []string{}
		// I assume arguments do not contain spaces
		for _, p := range strings.Split(text, " ") {
			// filter out empty strings
			// that string.Split does when encounters extra separators
			if len(p) > 0 {
				parts = append(parts, p)
			}
		}

		if len(parts) == 0 {
			continue
		}

		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "begin":
			_kv.Begin()
		case "commit":
			if !_kv.Commit() {
				fmt.Println("no transaction")
				continue
			}
		case "rollback":
			if !_kv.Rollback() {
				fmt.Println("no transaction")
				continue
			}
		case "get":
			if len(parts) != 2 {
				fmt.Println("GET should have exactly one argument")
				continue
			}

			if val, found := _kv.Get(parts[1]); found {
				fmt.Println(val)
			} else {
				fmt.Println("key not set")
				continue
			}
		case "set":
			if len(parts) != 3 {
				fmt.Println("SET should have exactly two arguments")
				continue
			}

			op := &kv.SetOperation{Key: parts[1], Value: parts[2]}
			_kv.AppendOperation(op)
			op.Apply(_kv)
		case "delete":
			if len(parts) != 2 {
				fmt.Println("DELETE should have exactly one argument")
				continue
			}

			op := &kv.DeleteOperation{Key: parts[1]}
			_kv.AppendOperation(op)
			op.Apply(_kv)
		case "count":
			if len(parts) != 2 {
				fmt.Println("COUNT should have exactly one argument")
				continue
			}

			fmt.Println(_kv.Count(parts[1]))
		}
	}
}