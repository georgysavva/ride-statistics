package main

import (
	"flag"
	"log"

	"github.com/georgysavva/ride-statistics/pkg/statistics"
)

func main() {
	var concurrency int
	flag.IntVar(&concurrency, "concurrency", 64, "number of workers that will process file in parallel")
	flag.Parse()

	inputFile, outputFile := "paths.csv", "statistics.csv"
	if flag.NArg() > 0 {
		inputFile = flag.Arg(0)
		if flag.NArg() > 1 {
			outputFile = flag.Arg(1)
		}
	}

	log.Printf(
		"Start calculating rides statitstics; input_file=%s, output_file=%s, concurrency=%d",
		inputFile, outputFile, concurrency,
	)
	if err := statistics.CalculateRidesStatistics(inputFile, outputFile, concurrency); err != nil {
		log.Fatal(err)
	}
}
