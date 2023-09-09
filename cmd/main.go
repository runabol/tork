package main

import (
	"fmt"
	"os"

	"github.com/runabol/tork/cli"
	"github.com/runabol/tork/conf"
)

func main() {
	if err := conf.LoadConfig(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err := cli.New().Run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
