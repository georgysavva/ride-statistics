package csvoutput_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/georgysavva/beat-test/pkg/statistics/internal/aggregation"
	"github.com/georgysavva/beat-test/pkg/statistics/internal/csvoutput"
)

func TestWriteCSVReport(t *testing.T) {
	t.Parallel()
	report := getTestReport()
	w := bytes.NewBufferString("")

	err := csvoutput.WriteCSVReport(w, report)
	require.NoError(t, err)
	actual := w.String()

	expected := `Time of Day,1 km,2 km,3 km,5 km,8 km,13 km,21 km,21+ km
00:00,1s,2s,3s,4s,5s,6s,7s,8s
01:00,11s,12s,13s,14s,15s,16s,17s,18s
02:00,21s,22s,23s,24s,25s,26s,27s,28s
03:00,31s,32s,33s,34s,35s,36s,37s,38s
04:00,41s,42s,43s,44s,45s,46s,47s,48s
05:00,51s,52s,53s,54s,55s,56s,57s,58s
06:00,1m1s,1m2s,1m3s,1m4s,1m5s,1m6s,1m7s,1m8s
07:00,1m11s,1m12s,1m13s,1m14s,1m15s,1m16s,1m17s,1m18s
08:00,1m21s,1m22s,1m23s,1m24s,1m25s,1m26s,1m27s,1m28s
09:00,1m31s,1m32s,1m33s,1m34s,1m35s,1m36s,1m37s,1m38s
10:00,1m41s,1m42s,1m43s,1m44s,1m45s,1m46s,1m47s,1m48s
11:00,1m51s,1m52s,1m53s,1m54s,1m55s,1m56s,1m57s,1m58s
12:00,2m1s,2m2s,2m3s,2m4s,2m5s,2m6s,2m7s,2m8s
13:00,2m11s,2m12s,2m13s,2m14s,2m15s,2m16s,2m17s,2m18s
14:00,2m21s,2m22s,2m23s,2m24s,2m25s,2m26s,2m27s,2m28s
15:00,2m31s,2m32s,2m33s,2m34s,2m35s,2m36s,2m37s,2m38s
16:00,2m41s,2m42s,2m43s,2m44s,2m45s,2m46s,2m47s,2m48s
17:00,2m51s,2m52s,2m53s,2m54s,2m55s,2m56s,2m57s,2m58s
18:00,3m1s,3m2s,3m3s,3m4s,3m5s,3m6s,3m7s,3m8s
19:00,3m11s,3m12s,3m13s,3m14s,3m15s,3m16s,3m17s,3m18s
20:00,3m21s,3m22s,3m23s,3m24s,3m25s,3m26s,3m27s,3m28s
21:00,3m31s,3m32s,3m33s,3m34s,3m35s,3m36s,3m37s,3m38s
22:00,3m41s,3m42s,3m43s,3m44s,3m45s,3m46s,3m47s,3m48s
23:00,3m51s,3m52s,3m53s,3m54s,3m55s,3m56s,3m57s,3m58s
`
	assert.Equal(t, expected, actual)
}

func getTestReport() aggregation.StatisticsReport {
	report := aggregation.StatisticsReport{}
	for i := 0; i < 24; i++ {
		report = append(report, &aggregation.HourStatistics{
			StartHour: i,
			DistanceStatistics: []*aggregation.DistanceStatistics{
				{DistanceRange: 1, Value: i*10 + 1},
				{DistanceRange: 2, Value: i*10 + 2},
				{DistanceRange: 3, Value: i*10 + 3},
				{DistanceRange: 5, Value: i*10 + 4},
				{DistanceRange: 8, Value: i*10 + 5},
				{DistanceRange: 13, Value: i*10 + 6},
				{DistanceRange: 21, Value: i*10 + 7},
				{DistanceRange: aggregation.DistanceRangeOver21KM, Value: i*10 + 8},
			},
		})
	}
	return report
}
