package ride_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/georgysavva/ride-statistics/pkg/statistics/internal/ride"
)

func TestStartRidesProcessors(t *testing.T) {
	t.Parallel()
	input := []*ride.Row{
		{RideID: 1, Lat: 37.966660, Lng: 23.728308, Timestamp: 1405594957},
		{RideID: 1, Lat: 37.967660, Lng: 23.727308, Timestamp: 1405594967},
		{RideID: 1, Lat: 37.968660, Lng: 23.726308, Timestamp: 1405594977},
		{RideID: 2, Lat: 37.966660, Lng: 23.728308, Timestamp: 1405594957},
		{RideID: 3, Lat: 37.966660, Lng: 23.728308, Timestamp: 1405594957},
		{RideID: 3, Lat: 37.966760, Lng: 23.727308, Timestamp: 1405594958},
	}
	inChan := make(chan *ride.Row, len(input))
	for _, ir := range input {
		inChan <- ir
	}
	close(inChan)

	outChan := make(chan *ride.Data)
	var actual []*ride.Data
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for rd := range outChan {
			actual = append(actual, rd)
		}
	}()
	wait := ride.StartRidesProcessors([]chan *ride.Row{inChan}, outChan)
	wait()
	wg.Wait()

	expected := []*ride.Data{
		{RideID: 1, StartTs: 1405594957, Distance: 282, Duration: 20},
		{RideID: 3, StartTs: 1405594957, Distance: 88, Duration: 1},
	}
	assert.Equal(t, expected, actual)
}
