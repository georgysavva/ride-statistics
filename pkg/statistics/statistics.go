package statistics

import (
	"github.com/pkg/errors"

	"github.com/georgysavva/beat-test/pkg/statistics/internal/aggregation"
	"github.com/georgysavva/beat-test/pkg/statistics/internal/csvoutput"
	"github.com/georgysavva/beat-test/pkg/statistics/internal/fileread"
	"github.com/georgysavva/beat-test/pkg/statistics/internal/ride"
)

const defaultBufferSize = 4096

func CalculateRidesStatistics(inputPath, outputPath string, concurrency int) error {
	if concurrency <= 0 {
		return errors.New("concurrency parameter must be a positive number")
	}

	rowsChannels := make([]chan *ride.Row, concurrency)
	for i := 0; i < concurrency; i++ {
		rowsChannels[i] = make(chan *ride.Row, defaultBufferSize/concurrency)
	}
	ridesChannel := make(chan *ride.Data, defaultBufferSize)

	fileReadersWait, err := fileread.StartFileReaders(inputPath, rowsChannels)
	if err != nil {
		return errors.Wrap(err, "can't start file readers")
	}

	calcWait := ride.StartRidesProcessors(rowsChannels, ridesChannel)

	aggregator := aggregation.NewRidesAggregator(ridesChannel)
	aggregator.StartCollecting()

	if err := fileReadersWait(); err != nil {
		return errors.Wrap(err, "file readers failed")
	}
	calcWait()
	aggregator.Finish()

	report := aggregator.Report95Percentile()
	if err := csvoutput.WriteCSVReportToFile(outputPath, report); err != nil {
		return errors.Wrap(err, "can't write report into output csv file")
	}
	return nil
}
