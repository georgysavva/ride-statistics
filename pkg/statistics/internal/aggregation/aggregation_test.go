package aggregation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/georgysavva/beat-test/pkg/statistics/internal/aggregation"
	"github.com/georgysavva/beat-test/pkg/statistics/internal/ride"
)

func TestRidesAggregator(t *testing.T) {
	t.Parallel()
	inputData := []*ride.Data{
		{RideID: 1, StartTs: 1609113888, Distance: 700, Duration: 600},
		{RideID: 2, StartTs: 1609113898, Distance: 1000, Duration: 700},
		{RideID: 3, StartTs: 1609113889, Distance: 1500, Duration: 800},
		{RideID: 4, StartTs: 1609113899, Distance: 2000, Duration: 900},
		{RideID: 5, StartTs: 1609117488, Distance: 800, Duration: 650},
		{RideID: 6, StartTs: 1609117498, Distance: 900, Duration: 750},
		{RideID: 7, StartTs: 1609117489, Distance: 1600, Duration: 850},
		{RideID: 8, StartTs: 1609117499, Distance: 1700, Duration: 950},
	}
	inCh := make(chan *ride.Data, len(inputData))
	for _, v := range inputData {
		inCh <- v
	}
	close(inCh)

	ra := aggregation.NewRidesAggregator(inCh)
	ra.StartCollecting()
	ra.Finish()
	actual := ra.Report95Percentile()

	expected := getTestReport()
	assert.Equal(t, expected, actual)
}

func getTestReport() aggregation.StatisticsReport {
	report := aggregation.StatisticsReport{
		{
			StartHour: 0,
			DistanceStatistics: []*aggregation.DistanceStatistics{
				{DistanceRange: 1, Value: 700},
				{DistanceRange: 2, Value: 900},
				{DistanceRange: 3, Value: 0},
				{DistanceRange: 5, Value: 0},
				{DistanceRange: 8, Value: 0},
				{DistanceRange: 13, Value: 0},
				{DistanceRange: 21, Value: 0},
				{DistanceRange: aggregation.DistanceRangeOver21KM, Value: 0},
			},
		},
		{
			StartHour: 1,
			DistanceStatistics: []*aggregation.DistanceStatistics{
				{DistanceRange: 1, Value: 750},
				{DistanceRange: 2, Value: 950},
				{DistanceRange: 3, Value: 0},
				{DistanceRange: 5, Value: 0},
				{DistanceRange: 8, Value: 0},
				{DistanceRange: 13, Value: 0},
				{DistanceRange: 21, Value: 0},
				{DistanceRange: aggregation.DistanceRangeOver21KM, Value: 0},
			},
		},
	}
	for i := 2; i < 24; i++ {
		report = append(report, &aggregation.HourStatistics{
			StartHour: i,
			DistanceStatistics: []*aggregation.DistanceStatistics{
				{DistanceRange: 1, Value: 0},
				{DistanceRange: 2, Value: 0},
				{DistanceRange: 3, Value: 0},
				{DistanceRange: 5, Value: 0},
				{DistanceRange: 8, Value: 0},
				{DistanceRange: 13, Value: 0},
				{DistanceRange: 21, Value: 0},
				{DistanceRange: aggregation.DistanceRangeOver21KM, Value: 0},
			},
		})
	}
	return report
}
