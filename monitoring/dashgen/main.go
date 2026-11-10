// Command dashgen generates the Kafka Lab dashboards, alert rules, rule tests and runbooks
// (`make dashboards` / `make alerts`) and lints them (`make dashboards-lint`).
//
//	go run ./monitoring/dashgen [-root DIR]         generate
//	go run ./monitoring/dashgen -lint [-root DIR]   lint files on disk
package main

import (
	"flag"
	"fmt"
	"os"

	"kafkalab/monitoring/dashgen/internal/app"
)

func main() {
	root := flag.String("root", ".", "repository root")
	doLint := flag.Bool("lint", false, "lint instead of generate")
	flag.Parse()
	if !*doLint {
		if err := app.Generate(*root, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "dashgen:", err)
			os.Exit(1)
		}
		return
	}
	problems := app.Lint(*root)
	for _, p := range problems {
		fmt.Println(p)
	}
	fmt.Printf("dashgen lint: %d problems\n", len(problems))
	if len(problems) > 0 {
		os.Exit(1)
	}
}
