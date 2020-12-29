package ride

import (
	"log"
	"sync"
)

type Data struct {
	RideID   int
	StartTs  int
	Distance int
	Duration int
}

type Row struct {
	RideID    int
	Lat       float64
	Lng       float64
	Timestamp int
}

func StartRidesProcessors(ins []chan *Row, out chan<- *Data) func() {
	wg := &sync.WaitGroup{}
	wg.Add(len(ins))
	for _, in := range ins {
		go func(in <-chan *Row) {
			defer wg.Done()
			processRides(in, out)
		}(in)
	}
	return func() {
		defer close(out)
		wg.Wait()
	}
}

func processRides(in <-chan *Row, out chan<- *Data) {
	var (
		lastRow     *Row
		currentRide *Data
	)
	for row := range in {
		if 0 > row.Lat || row.Lat > 90 || 0 > row.Lng || row.Lng > 90 {
			log.Printf("Input row contains invlide lat or lng: %+v", row)
			continue
		}
		if lastRow != nil {
			if lastRow.RideID == row.RideID {
				if currentRide == nil {
					currentRide = &Data{
						RideID:  lastRow.RideID,
						StartTs: lastRow.Timestamp,
					}
				}
				currentRide.Duration += row.Timestamp - lastRow.Timestamp
				currentRide.Distance += int(calculateDistance(row.Lat, row.Lng, lastRow.Lat, lastRow.Lng))
			} else if currentRide != nil {
				out <- currentRide
				currentRide = nil
			}
		}
		lastRow = row
	}
	if currentRide != nil {
		out <- currentRide
		currentRide = nil
	}
}
