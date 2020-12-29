package fileread

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/georgysavva/beat-test/pkg/statistics/internal/ride"
)

const (
	SimpleInputFile = "testdata/simple_input.csv"
	LineSize        = 33
)

func TestReadRidesSequence(t *testing.T) {
	t.Parallel()
	b, err := ioutil.ReadFile(SimpleInputFile)
	require.NoError(t, err)
	totalSize := len(b)
	r := bytes.NewReader(b)
	ctx := context.Background()
	cases := []struct {
		name     string
		chunk    *fileChunk
		expected []*ride.Row
	}{
		{
			name: "chunk starts at the beginning of the file and ends 1.5 line before the 2th ride - " +
				"only the 1th ride must be processed",
			chunk: &fileChunk{
				start: 0,
				size:  LineSize + LineSize/2,
			},
			expected: []*ride.Row{
				{1, 37.966660, 23.728308, 1405594957},
				{1, 37.966627, 23.728263, 1405594966},
				{1, 37.966625, 23.728264, 1405594974},
			},
		},
		{
			name: "chunk starts 1.5 line before the 2th ride and ends 1.5 line before the 3th ride - " +
				"only the 2th ride must be processed",
			chunk: &fileChunk{
				start: LineSize + LineSize/2,
				size:  3 * LineSize,
			},
			expected: []*ride.Row{
				{2, 37.946413, 23.754767, 1405591094},
				{2, 37.946260, 23.754830, 1405591103},
				{2, 37.946032, 23.755347, 1405591112},
			},
		},
		{
			name: "chunk starts 1 line before the 2th ride before the line delimiter and ends 1.5 line before the 3th ride - " +
				"only the 2th ride must be processed",
			chunk: &fileChunk{
				start: 2*LineSize - 2,
				size:  2*LineSize + LineSize/2,
			},
			expected: []*ride.Row{
				{2, 37.946413, 23.754767, 1405591094},
				{2, 37.946260, 23.754830, 1405591103},
				{2, 37.946032, 23.755347, 1405591112},
			},
		},
		{
			name: "chunk starts 1 line before the 2th ride at the line delimiter and ends 1.5 line before the 3th ride - " +
				"only the 2th ride must be processed",
			chunk: &fileChunk{
				start: 2*LineSize - 1,
				size:  2*LineSize + LineSize/2,
			},
			expected: []*ride.Row{
				{2, 37.946413, 23.754767, 1405591094},
				{2, 37.946260, 23.754830, 1405591103},
				{2, 37.946032, 23.755347, 1405591112},
			},
		},
		{
			name: "chunk starts 1 line before the 2th ride after the line delimiter and ends 1.5 line before the 3th ride - " +
				"no rides must be processed",
			chunk: &fileChunk{
				start: 2 * LineSize,
				size:  2*LineSize + LineSize/2,
			},
			expected: nil,
		},
		{
			name: "chunk starts 1.5 line before the 2th ride and ends 1 line before the 3th ride before the line delimiter - " +
				"only the 2th ride must be processed",
			chunk: &fileChunk{
				start: LineSize + LineSize/2,
				size:  5*LineSize - (LineSize + LineSize/2) - 2,
			},
			expected: []*ride.Row{
				{2, 37.946413, 23.754767, 1405591094},
				{2, 37.946260, 23.754830, 1405591103},
				{2, 37.946032, 23.755347, 1405591112},
			},
		},
		{
			name: "chunk starts 1.5 line before the 2th ride and ends 1 line before the 3th ride at the line delimiter - " +
				"2th, 3th rides must be processed",
			chunk: &fileChunk{
				start: LineSize + LineSize/2,
				size:  5*LineSize - (LineSize + LineSize/2),
			},
			expected: []*ride.Row{
				{2, 37.946413, 23.754767, 1405591094},
				{2, 37.946260, 23.754830, 1405591103},
				{2, 37.946032, 23.755347, 1405591112},
				{3, 37.926738, 23.935701, 1405591810},
				{3, 37.927245, 23.935000, 1405591818},
				{3, 37.926763, 23.934286, 1405591827},
			},
		},
		{
			name: "chunk starts 1.5 line before the 2th ride and ends 1 line before the 3th ride after the line delimiter - " +
				"2th, 3th rides must be processed",
			chunk: &fileChunk{
				start: LineSize + LineSize/2,
				size:  5*LineSize - (LineSize + LineSize/2) + 1,
			},
			expected: []*ride.Row{
				{2, 37.946413, 23.754767, 1405591094},
				{2, 37.946260, 23.754830, 1405591103},
				{2, 37.946032, 23.755347, 1405591112},
				{3, 37.926738, 23.935701, 1405591810},
				{3, 37.927245, 23.935000, 1405591818},
				{3, 37.926763, 23.934286, 1405591827},
			},
		},
		{
			name: "chunk starts at the beginning of the file and ends 0.5 line before the 2th ride - " +
				"1th, 2th rides must be processed",
			chunk: &fileChunk{
				start: 0,
				size:  2*LineSize + LineSize/2,
			},
			expected: []*ride.Row{
				{1, 37.966660, 23.728308, 1405594957},
				{1, 37.966627, 23.728263, 1405594966},
				{1, 37.966625, 23.728264, 1405594974},
				{2, 37.946413, 23.754767, 1405591094},
				{2, 37.946260, 23.754830, 1405591103},
				{2, 37.946032, 23.755347, 1405591112},
			},
		},
		{
			name: "chunk starts 1.5 line before the 3th ride and ends at the EOF - " +
				"only the 3th ride must be processed",
			chunk: &fileChunk{
				start: 4*LineSize + LineSize/2,
				size:  totalSize - (4*LineSize + LineSize/2),
			},
			expected: []*ride.Row{
				{3, 37.926738, 23.935701, 1405591810},
				{3, 37.927245, 23.935000, 1405591818},
				{3, 37.926763, 23.934286, 1405591827},
			},
		},
		{
			name: "empty chunk starts at the EOF - no rides must be processed",
			chunk: &fileChunk{
				start: totalSize,
				size:  0,
			},
			expected: nil,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := make(chan *ride.Row, len(tc.expected))
			var actual []*ride.Row
			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for v := range out {
					actual = append(actual, v)
				}
			}()
			err := readRidesSequence(ctx, r, totalSize, tc.chunk, out)
			require.NoError(t, err)
			wg.Wait()

			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestSplitFile(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		fileSize int
		chunksNo int
		expected []*fileChunk
	}{
		{
			name:     "not equal split",
			fileSize: 17,
			chunksNo: 3,
			expected: []*fileChunk{
				{start: 0, size: 6},
				{start: 6, size: 6},
				{start: 12, size: 5},
			},
		},
		{
			name:     "equal split",
			fileSize: 18,
			chunksNo: 3,
			expected: []*fileChunk{
				{start: 0, size: 6},
				{start: 6, size: 6},
				{start: 12, size: 6},
			},
		},
		{
			name:     "not enough for every chunk",
			fileSize: 2,
			chunksNo: 4,
			expected: []*fileChunk{
				{start: 0, size: 1},
				{start: 1, size: 1},
				{start: 2, size: 0},
				{start: 2, size: 0},
			},
		},
		{
			name:     "single chunk",
			fileSize: 10,
			chunksNo: 1,
			expected: []*fileChunk{
				{start: 0, size: 10},
			},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			actual := splitFile(tc.fileSize, tc.chunksNo)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestReadLine(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		content     string
		expected    string
		expectedErr error
	}{
		{
			name:        "EOF after delimiter",
			content:     "foo\n",
			expected:    "foo\n",
			expectedErr: nil,
		},
		{
			name:        "EOF without delimiter",
			content:     "foo",
			expected:    "foo",
			expectedErr: nil,
		},
		{
			name:        "EOF after delimiter empty content",
			content:     "\n",
			expected:    "\n",
			expectedErr: nil,
		},
		{
			name:        "EOF without delimiter empty content",
			content:     "",
			expected:    "",
			expectedErr: io.EOF,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			actual, err := readLine(bufio.NewReader(strings.NewReader(tc.content)))
			assert.Equal(t, tc.expected, actual)
			assert.Equal(t, tc.expectedErr, err)
		})
	}
}
