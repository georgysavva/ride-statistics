package fileread_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/georgysavva/beat-test/pkg/statistics/internal/fileread"
	"github.com/georgysavva/beat-test/pkg/statistics/internal/ride"
)

func TestStartFileReaders(t *testing.T) {
	t.Parallel()
	expected := [][]*ride.Row{
		{
			{1, 37.966660, 23.728308, 1405594957},
			{1, 37.966627, 23.728263, 1405594966},
			{1, 37.966625, 23.728264, 1405594974},
			{2, 37.946413, 23.754767, 1405591094},
			{2, 37.946260, 23.754830, 1405591103},
			{2, 37.946032, 23.755347, 1405591112},
		},
		{
			{3, 37.926738, 23.935701, 1405591810},
			{3, 37.927245, 23.935000, 1405591818},
			{3, 37.926763, 23.934286, 1405591827},
		},
	}
	actual := make([][]*ride.Row, len(expected))
	outs := make([]chan *ride.Row, len(expected))
	wg := &sync.WaitGroup{}
	wg.Add(len(outs))
	for i := range outs {
		outs[i] = make(chan *ride.Row, len(expected[i]))
		go func(i int) {
			defer wg.Done()
			for v := range outs[i] {
				actual[i] = append(actual[i], v)
			}
		}(i)
	}

	wait, err := fileread.StartFileReaders(fileread.SimpleInputFile, outs)
	require.NoError(t, err)
	err = wait()
	require.NoError(t, err)
	wg.Wait()

	assert.Equal(t, expected, actual)
}
