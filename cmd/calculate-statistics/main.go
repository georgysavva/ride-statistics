package main

import (
	"log"

	"github.com/alexflint/go-arg"

	"github.com/georgysavva/ride-statistics/pkg/statistics"
)

type Args struct {
	Concurrency int    `default:"64" help:"number of workers that will process file in parallel"`
	InputFile   string `arg:"positional" default:"recorded_rides.csv" help:"path to the input csv file with recorded rides [default: recorded_rides.csv]"` // nolint: lll
	OutputFile  string `arg:"positional" default:"statistics.csv" help:"path to the output csv file to write statistics to [default: statistics.csv]"`     // nolint: lll
}

func main() {
	args := &Args{}
	arg.MustParse(args)

	log.Printf(
		"Start calculating rides statitstics; input_file=%s, output_file=%s, concurrency=%d",
		args.InputFile, args.OutputFile, args.Concurrency,
	)
	if err := statistics.CalculateRidesStatistics(args.InputFile, args.OutputFile, args.Concurrency); err != nil {
		log.Fatal(err)
	}
}
