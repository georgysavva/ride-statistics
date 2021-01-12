# Overview
Let's assume we have a ride hailing service and we want to give our passenger a ride duration prediction. 
To do that we need to have some statistics based on previously recorded rides.

This repository contains a highly efficient parallel script to calculate the statistics of previously recorded rides. 
Each recorded ride consists of a series of entries. Each entry contains the unique identifier of
the ride, the coordinates of a point (lat, lng) and a UNIX timestamp. 
Recorded rides are provided as an input CSV file with the following columns: RideID, Lat, Lng, Timestamp.
CSV records are ordered by RideID and Timestamp columns.

Given the above data as an input file, the script produces a CSV report that shows the
95th percentile of ride duration for the rides, distributed across the hours of the day according to
their start time and for ride distance ranges of 1, 2, 3, 5, 8, 13, 21 and over 21 km.

## Implementation details

To ensure high efficiency and full utilization of a modern multicore system the script processes the input file in parallel.
Processing consists of several pipelined stages. 

The first stage is reading the file in parallel. 
Script splits the input file into even chunks and spawns a dedicated goroutine for each chunk 
responsible for reading rows from the chunk and sending them into an individual channel to the next stage. 
The restriction for splitting into chunks is that rows belonging to the same ride must be within a single chunk. 
We need that restriction and individual channel for each chunk to ensure that all ride rows will end up in the same channel 
and in the original order 
To satisfy the restriction chunk's start and end positions are dynamically adjusted by the dedicated goroutine.
"concurrency" parameter controls the number of chunks and hence the number of parallel readying goroutines.

The second stage consists of parallel workers, the amount of workers is equal to the number of chunks 
and they read from their corresponding chunk channel.
Each worker processes ride rows from the channel sequentially and calculates the final ride data: distance, duration, start time.
After the ride data is calculated workers send it to the single channel to the next stage 
because we don't depend on data order any more.

The third stage consists of multiple parallel workers responsible for receiving the ride data and aggregating it by the start time and distance. 
After all rides data is collected it reports the 95th percentile for each start hour and distance range.

## Setup and run

In the project root do:

- Run all tests: `go test -race ./...`
- Build the binary `go build -o ./ ./...`
- Run the calculation script with default parameters `./calculate-statistics`
