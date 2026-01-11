package main

import (
	"log"
	"math/rand"
	"s3_sink_reference/pkg/morsel"
	"s3_sink_reference/pkg/s3"
	"s3_sink_reference/pkg/sink"
	"sync"
	"time"
)

func main() {
	log.Println("Starting S3 Sink Reference Benchmark...")

	// 1. Setup
	writer := s3.NewMockWriter("benchmark-bucket", "us-east-1")
	s3Sink := sink.NewS3Sink(writer, "benchmark-bucket", "output.parquet")
	s3Sink.Start()

	// 2. Simulation Parameters
	numProducers := 4
	totalMorsels := 256
	morselSize := 1024 * 1024 // 1MB per morsel
	
	start := time.Now()

	// 3. Run Producers
	var wg sync.WaitGroup
	wg.Add(numProducers)

	log.Printf("Spawning %d producers, aiming for %d MB total...", numProducers, (totalMorsels*morselSize)/(1024*1024))

	for i := 0; i < numProducers; i++ {
		go func(id int) {
			defer wg.Done()
			morselsPerProducer := totalMorsels / numProducers
			
			// Simulate compute time
			data := make([]byte, morselSize)
			rand.Read(data) // Fill with random data once

			for j := 0; j < morselsPerProducer; j++ {
				// Artificial compute delay
				time.Sleep(10 * time.Millisecond)

				m := morsel.Morsel{
					Data: data,
					Meta: morsel.RowGroupMeta{NumRows: 1000, TotalByteSize: int64(morselSize)},
				}
				s3Sink.Submit(m)
			}
			log.Printf("Producer %d finished", id)
		}(i)
	}

	wg.Wait()
	log.Println("All producers finished. Closing sink...")

	// 4. Close and Wait
	if err := s3Sink.Close(); err != nil {
		log.Fatalf("Sink failed: %v", err)
	}

	elapsed := time.Since(start)
	totalBytes := int64(totalMorsels * morselSize)
	mb := float64(totalBytes) / (1024 * 1024)
	throughput := mb / elapsed.Seconds()

	log.Printf("Benchmark Complete!")
	log.Printf("Total Data: %.2f MB", mb)
	log.Printf("Time: %.2fs", elapsed.Seconds())
	log.Printf("Throughput: %.2f MB/s", throughput)
}
