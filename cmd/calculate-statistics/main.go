package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/georgysavva/ride-statistics/pkg/statistics"
)

func main() {
	var concurrency int
	flag.IntVar(&concurrency, "concurrency", 64, "number of workers that will process file in parallel")
	flag.Usage = func() {
		fmt.Fprintf(
			flag.CommandLine.Output(),
			"Usage: %s [options] [input_file] [output_file]\n\n"+
				"Positional arguments:\n"+
				"1th argument, input_file - "+
				"path to the input csv file with recorded rides (default recorded_rides.csv)\n"+
				"2th argument, output_file - "+
				"path to the output csv file to write statistics to (default statistics.csv)\n\n"+
				"Options:\n",
			os.Args[0],
		)
		flag.PrintDefaults()
	}
	flag.Parse()

	inputFile, outputFile := "recorded_rides.csv", "statistics.csv"
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
