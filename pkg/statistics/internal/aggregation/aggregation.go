package aggregation

import (
	"fmt"
	"log"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/emirpasic/gods/maps/treemap"

	"github.com/georgysavva/ride-statistics/pkg/statistics/internal/ride"
)

type StatisticsReport []*HourStatistics

type HourStatistics struct {
	StartHour          int
	DistanceStatistics []*DistanceStatistics
}

type DistanceStatistics struct {
	DistanceRange int
	Value         int
}

var distanceRanges = [...]int{1, 2, 3, 5, 8, 13, 21, DistanceRangeOver21KM}

const (
	DistanceRangeOver21KM = math.MaxInt64
	hoursRangesNo         = 24
	percentile95          = 0.95
	cellsNo               = len(distanceRanges) * hoursRangesNo
)

type RidesAggregator struct {
	wg   *sync.WaitGroup
	inCh <-chan *ride.Data

	// cells are two level nested sorted map
	// where the first dimension is hours ranges and the second dimension is distance ranges.
	// Each individual cell contains a list of all collected durations for cell's hour and distance ranges.
	cells *treemap.Map
}

func NewRidesAggregator(in <-chan *ride.Data) *RidesAggregator {
	cells := treemap.NewWithIntComparator()
	for startHour := 0; startHour < hoursRangesNo; startHour++ {
		cellsPerHour := treemap.NewWithIntComparator()
		for _, startDistance := range distanceRanges {
			cellsPerHour.Put(startDistance, &aggregationCell{mx: new(sync.Mutex)})
		}
		cells.Put(startHour, cellsPerHour)
	}
	return &RidesAggregator{
		inCh:  in,
		cells: cells,
		wg:    new(sync.WaitGroup),
	}
}

func (ra *RidesAggregator) StartCollecting() {
	workersNum := cellsNo
	ra.wg.Add(workersNum)
	for i := 0; i < workersNum; i++ {
		go func() {
			defer ra.wg.Done()
			ra.writeRideDataToCell()
		}()
	}
}

func (ra *RidesAggregator) Finish() {
	ra.wg.Wait()

	finishWG := &sync.WaitGroup{}
	finishWG.Add(cellsNo)
	ra.cells.Each(func(_ interface{}, hourCellsValue interface{}) {
		hourCells := hourCellsValue.(*treemap.Map)
		hourCells.Each(func(_ interface{}, cellValue interface{}) {
			cell := cellValue.(*aggregationCell)
			go func() {
				defer finishWG.Done()
				cell.sort()
			}()
		})
	})
	finishWG.Wait()
}

func (ra *RidesAggregator) Report95Percentile() StatisticsReport {
	report := make(StatisticsReport, 0, ra.cells.Size())
	ra.cells.Each(func(hourKey interface{}, hourCellsValue interface{}) {
		startHour := hourKey.(int)
		hourCells := hourCellsValue.(*treemap.Map)
		hs := &HourStatistics{
			StartHour:          startHour,
			DistanceStatistics: make([]*DistanceStatistics, 0, hourCells.Size()),
		}
		report = append(report, hs)
		hourCells.Each(func(distanceKey interface{}, cellValue interface{}) {
			distanceRange := distanceKey.(int)
			cell := cellValue.(*aggregationCell)
			ds := &DistanceStatistics{
				DistanceRange: distanceRange,
				Value:         cell.get95Percentile(),
			}
			hs.DistanceStatistics = append(hs.DistanceStatistics, ds)
		})
	})
	return report
}

type aggregationCell struct {
	durations []int
	mx        *sync.Mutex
}

func (ac *aggregationCell) add(duration int) {
	ac.mx.Lock()
	defer ac.mx.Unlock()
	ac.durations = append(ac.durations, duration)
}

func (ac *aggregationCell) sort() {
	sort.Ints(ac.durations)
}

func (ac *aggregationCell) get95Percentile() int {
	if len(ac.durations) == 0 {
		return 0
	}
	idx := int(math.Round(float64(len(ac.durations)) * percentile95))
	if idx == len(ac.durations) {
		idx--
	}
	return ac.durations[idx]
}

func (ra *RidesAggregator) writeRideDataToCell() {
	for data := range ra.inCh {
		if data.Distance < 0 || data.StartTs < 0 || data.Duration < 0 {
			log.Printf("Ride data is invalid, skip it: %+v", data)
			continue
		}
		startTimeUTC := time.Unix(int64(data.StartTs), 0).UTC()
		startHour := startTimeUTC.Hour()
		hourCellsValue, found := ra.cells.Get(startHour)
		if !found {
			panic(fmt.Sprintf("can't find map value for hour %d, map keys: %v", startHour, ra.cells.Keys()))
		}
		hourCells := hourCellsValue.(*treemap.Map)
		const metersInKm = 1000
		distanceKM := int(math.Round(float64(data.Distance) / metersInKm))
		_, cellValue := hourCells.Ceiling(distanceKM)
		if cellValue == nil {
			panic(fmt.Sprintf("can't find map value for distance %d, map keys: %v", distanceKM, hourCells.Keys()))
		}
		cell := cellValue.(*aggregationCell)
		cell.add(data.Duration)
	}
}
