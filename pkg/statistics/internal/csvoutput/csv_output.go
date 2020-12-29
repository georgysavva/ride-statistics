package csvoutput

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/pkg/errors"

	"github.com/georgysavva/beat-test/pkg/statistics/internal/aggregation"
)

func WriteCSVReportToFile(filePath string, report aggregation.StatisticsReport) error {
	f, err := os.Create(filePath)
	if err != nil {
		return errors.Wrap(err, "can't open output file for writing")
	}
	defer f.Close() // nolint: errcheck, gosec
	if err := WriteCSVReport(f, report); err != nil {
		return errors.WithStack(err)
	}
	if err := f.Close(); err != nil {
		return errors.Wrap(err, "can't close output csv file")
	}
	return nil
}

func WriteCSVReport(w io.Writer, report aggregation.StatisticsReport) error {
	csvw := csv.NewWriter(w)
	for i, hs := range report {
		if i == 0 {
			records := []string{"Time of Day"}
			for _, ds := range hs.DistanceStatistics {
				var dr string
				if ds.DistanceRange == aggregation.DistanceRangeOver21KM {
					dr = "21+"
				} else {
					dr = strconv.Itoa(ds.DistanceRange)
				}
				records = append(records, fmt.Sprintf("%s km", dr))
			}
			if err := csvw.Write(records); err != nil {
				return errors.Wrap(err, "can't write csv header")
			}
		}
		records := []string{fmt.Sprintf("%02d:00", hs.StartHour)}
		for _, ds := range hs.DistanceStatistics {
			records = append(records, (time.Duration(ds.Value) * time.Second).String())
		}
		if err := csvw.Write(records); err != nil {
			return errors.Wrap(err, "can't write report to csv")
		}
	}
	csvw.Flush()
	if err := csvw.Error(); err != nil {
		return errors.Wrap(err, "can't write report to csv")
	}
	return nil
}
