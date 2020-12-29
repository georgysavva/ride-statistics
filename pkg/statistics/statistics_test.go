package statistics_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/georgysavva/beat-test/pkg/statistics"
)

func TestCalculateRidesStatistics(t *testing.T) {
	t.Parallel()
	expectedBytes, err := ioutil.ReadFile("testdata/statistics_output.golden.csv")
	require.NoError(t, err)
	expected := string(expectedBytes)

	cases := []struct {
		concurrency int
	}{
		{concurrency: 1},
		{concurrency: 2},
		{concurrency: 3},
		{concurrency: 4},
		{concurrency: 5},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(fmt.Sprintf("concurrency=%d", tc.concurrency), func(t *testing.T) {
			t.Parallel()
			outputFile, err := ioutil.TempFile("", "statistics_output_*.csv")
			require.NoError(t, err)
			defer require.NoError(t, os.Remove(outputFile.Name()))

			err = statistics.CalculateRidesStatistics("testdata/complete_input.csv", outputFile.Name(), tc.concurrency)
			require.NoError(t, err)

			actualBytes, err := ioutil.ReadFile(outputFile.Name())
			require.NoError(t, err)
			actual := string(actualBytes)
			assert.Equal(t, expected, actual)
		})
	}
}
