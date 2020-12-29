package fileread

import (
	"bufio"
	"context"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/georgysavva/beat-test/pkg/statistics/internal/ride"
)

func StartFileReaders(filePath string, outs []chan *ride.Row) (func() error, error) {
	if len(outs) == 0 {
		return nil, errors.New("slice of out channels can't be empty")
	}
	f, err := os.Open(path.Clean(filePath))
	if err != nil {
		return nil, errors.Wrap(err, "can't open input csv file")
	}
	fileStat, err := f.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "can't get input csv file stat")
	}
	fileSize := int(fileStat.Size())
	// Equally split the input file into chunks, 1 chunk for each out channel.
	chunks := splitFile(fileSize, len(outs))
	eg, ctx := errgroup.WithContext(context.Background())
	if len(chunks) > 1 {
		for i, chunk := range chunks {
			chunk := chunk
			out := outs[i]
			eg.Go(func() error {
				return errors.WithStack(readRidesSequence(ctx, f, fileSize, chunk, out))
			})
		}
	} else {
		eg.Go(func() error {
			return errors.WithStack(readAllRidesSequence(ctx, f, outs[0]))
		})
	}
	return func() error {
		defer f.Close() // nolint: errcheck, gosec
		if err := eg.Wait(); err != nil {
			return errors.WithStack(err)
		}
		if err := f.Close(); err != nil {
			return errors.Wrap(err, "can't close input csv file")
		}
		return nil
	}, nil
}

func readAllRidesSequence(ctx context.Context, f io.Reader, out chan<- *ride.Row) error {
	defer close(out)
	r := bufio.NewScanner(f)
	for r.Scan() {
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		default:
		}
		s := r.Text()
		row, err := parseRow(s)
		if err != nil {
			return errors.WithStack(err)
		}
		out <- row
	}
	if err := r.Err(); err != nil {
		return errors.Wrap(err, "file scanner final error")
	}
	return nil
}

func readRidesSequence(ctx context.Context, f io.ReaderAt, totalSize int, chunk *fileChunk, out chan<- *ride.Row,
) error {
	defer close(out)
	sr := io.NewSectionReader(f, int64(chunk.start), int64(totalSize))
	r := bufio.NewReader(sr)
	var (
		bytesRead int

		// This variable indicates whether a new ride sequence started
		// and we should start sending rows to the out channel or not
		sequenceStarted bool
	)

	// If it's the first chunk, sequence is always started.
	// For non-first chunks we need to skip some bytes until '\n' to start from a new row beginning.
	if chunk.start == 0 {
		sequenceStarted = true
	} else {
		s, err := r.ReadString('\n')
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return errors.Wrap(err, "can't skip bytes at the chunk start")
		}
		bytesRead += len(s)
	}

	currentRow, currentRowSize, err := getRow(r)
	if err != nil && !errors.Is(err, io.EOF) {
		return errors.WithStack(err)
	}
	finish := errors.Is(err, io.EOF)
	for !finish {
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		default:
		}
		// Determine if currentRow is completely outside of the chunk range.
		chunkSizeExceeded := bytesRead > chunk.size

		// If the chunk size exceeded and sequence never started
		// it means that we don't need to continue iterating until a different ride id and can stop right away.
		if !sequenceStarted && chunkSizeExceeded {
			break
		}
		// If we were able to capture the start of a new ride sequence we can start sending rows to the out channel.
		if sequenceStarted {
			out <- currentRow
		}
		nextRow, nextRowSize, err := getRow(r)
		if err != nil && !errors.Is(err, io.EOF) {
			return errors.WithStack(err)
		}
		// If there is no next row or the chunk size is exceeded and the next has a different ride id
		// We can finish iteration after current cycle.
		finish = errors.Is(err, io.EOF) || (chunkSizeExceeded && currentRow.RideID != nextRow.RideID)

		if !finish {
			// Try to capture the start of a new ride sequence to be able to start sending rows to the out channel.
			if !sequenceStarted && currentRow.RideID != nextRow.RideID {
				sequenceStarted = true
			}
		}
		bytesRead += currentRowSize
		currentRow = nextRow
		currentRowSize = nextRowSize
	}
	return nil
}

func getRow(r *bufio.Reader) (*ride.Row, int, error) {
	s, err := readLine(r)
	if err != nil {
		return nil, 0, err
	}
	row, err := parseRow(s)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	return row, len(s), nil
}

func readLine(r *bufio.Reader) (string, error) {
	s, err := r.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", errors.Wrap(err, "read line failed")
	}
	if errors.Is(err, io.EOF) && s == "" {
		return "", io.EOF
	}
	return s, nil
}

func parseRow(s string) (*ride.Row, error) {
	s = strings.TrimSpace(s)
	columns := strings.Split(s, ",")
	const csvColumnsNo = 4
	if len(columns) != csvColumnsNo {
		return nil, errors.Errorf("not enough columns in csv row: %s", s)
	}
	rideID, err := strconv.Atoi(columns[0])
	if err != nil {
		return nil, errors.Wrap(err, "can't parse rideID column")
	}
	lat, err := strconv.ParseFloat(columns[1], 64 /* bitSize */)
	if err != nil {
		return nil, errors.Wrap(err, "can't parse lat column")
	}
	lng, err := strconv.ParseFloat(columns[2], 64 /* bitSize */)
	if err != nil {
		return nil, errors.Wrap(err, "can't parse lng column")
	}
	timestamp, err := strconv.Atoi(columns[3])
	if err != nil {
		return nil, errors.Wrap(err, "can't parse timestamp column")
	}
	return &ride.Row{
		RideID:    rideID,
		Lat:       lat,
		Lng:       lng,
		Timestamp: timestamp,
	}, nil
}

type fileChunk struct {
	start int
	size  int
}

func splitFile(fileSize, chunksNo int) []*fileChunk {
	var currentOffset int
	baseChunkSize := fileSize / chunksNo
	remainingSize := fileSize % chunksNo
	chunks := make([]*fileChunk, chunksNo)
	for i := 0; i < chunksNo; i++ {
		chunkSize := baseChunkSize
		if remainingSize != 0 {
			chunkSize++
			remainingSize--
		}
		chunks[i] = &fileChunk{
			start: currentOffset,
			size:  chunkSize,
		}
		currentOffset += chunkSize
	}
	return chunks
}
