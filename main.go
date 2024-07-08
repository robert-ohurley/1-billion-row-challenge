/*
The task is to write a Java program which reads the file, calculates the min, mean, and max temperature value per weather station,
and emits the results on stdout like this (i.e. sorted alphabetically by station name, and the result values per station in the format <min>/<mean>/<max>,
rounded to one fractional digit):
*/
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"
)

var GlobalLock = &sync.Mutex{}
var BuffersAllocated = 0
var Returned = 0

// Optimisation: Reusing buffers after having been scanned reduces memory allocation from
// approx 60,000 buffers of 512 x 512kb to approx 600.
// Reduces memory allocation from ~15gb to ~125mb
var BufferPool = &sync.Pool{
	New: func() interface{} {
		GlobalLock.Lock()
		BuffersAllocated++
		GlobalLock.Unlock()
		return make([]byte, 0, BUFFER_SIZE)
	},
}

const (
	BUFFER_SIZE = 512 * 512
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

type Tally struct {
	results map[string]*StationResult
}

var FinalTally Tally = Tally{
	make(map[string]*StationResult),
}

// min, max and sum are all multiplied by ten to avoid floating point arithmetic
type StationResult struct {
	min, max, sum, count int
	m                    *sync.Mutex
}

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	filePtr, err := os.Open("./measurements.txt")

	if err != nil {
		log.Fatal("Error reading file")
	}

	start := time.Now()

	//Optimisation: Multithreading application.
	//Use channels to synchronise
	linesCh := readInFile(filePtr)
	out := parseCh(linesCh)

	<-out
	elapsed := time.Since(start)
	fmt.Println(elapsed)
	fmt.Println("Alloc: ", BuffersAllocated)
	fmt.Println("Returned: ", Returned)

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		pprof.Lookup("allocs").WriteTo(f, 0)
	}
}

func parseCh(in <-chan []byte) <-chan int {
	out := make(chan int)
	wg := &sync.WaitGroup{}

	go func() {
		for chunk := range in {
			wg.Add(1)
			go parseLines(chunk, wg)
		}
		wg.Wait()
		out <- 1
		close(out)
	}()

	return out
}

func parseLines(chunk []byte, wg *sync.WaitGroup) {
	defer wg.Done()

	scanner := bufio.NewScanner(bytes.NewReader(chunk))

	for scanner.Scan() {
		b := scanner.Bytes()

		semiColonIdx := -1

		for i, b := range b {
			if b == ';' {
				semiColonIdx = i
			}
		}

		if semiColonIdx == -1 {
			continue
		}

		station := b[0:semiColonIdx]

		stationTemp := 0

		//Optimisation: parse backwards over the float and do integer arithmetic to avoid floating point arithmetic
		for i := len(b) - 1; i > semiColonIdx; i-- {
			exponent := 0

			if b[i] == '.' {
				continue
			}

			if b[i] == '-' {
				stationTemp *= -1
			}

			stationTemp += int(b[i]) * (10 ^ int(exponent))
			exponent++
		}

		result, ok := FinalTally.results[string(station)]
		if !ok {
			result = &StationResult{
				0, 0, 0, 0, &sync.Mutex{},
			}
		}

		result.m.Lock()
		if stationTemp > result.max {
			result.max = stationTemp
		}

		if stationTemp < result.min {
			result.min = stationTemp
		}

		result.count++

		result.sum += stationTemp

		result.m.Unlock()
	}

	//Return chunked buffer to pool
	BufferPool.Put(chunk)
}

func readInFile(filePtr *os.File) <-chan []byte {
	//Optimisation: Read into a single buffer, clone the results into a channel
	//Works best with approx 512kb x 512kb buffer size
	buffer := make([]byte, BUFFER_SIZE)
	fragment := make([]byte, 1024)

	//fragLength is the value returned by copy otherwise I would just do len(fragment)
	fragLength := 0
	out := make(chan []byte)

	go func() {
		for {
			//clone := make([]byte, BUFFER_SIZE)
			//Buffer only gets returned to the pool when a scanner has read all it's bytes
			//Length is reset to zero
			clone := BufferPool.Get().([]byte)[0:0]

			//If any bytes are in the fragment, prepend to clone and resume copying after.
			if len(fragment) > 0 {
				fragLength = copy(clone, fragment)
				fragment = fragment[0:0]
			}

			//Read file into the buffer starting after the length of the fragment which was copied in.
			n, err := filePtr.Read(buffer[fragLength:])

			if err == io.EOF {
				break
			}

			//Optimisation: Read backwards over the partial line and copy it into the fragment buffer for use next time through.
			if buffer[n-1] != byte('\n') {
				for i := n - 1; i >= 0; i-- {
					if buffer[i] == byte('\n') {
						copy(fragment, buffer[i:])
						n = i
						break
					}
				}
			}

			//Copy the buffer content into a new buffer up until the beginning of the fragment
			copy(clone, buffer[:n])

			out <- clone
		}
		close(out)
	}()
	return out
}
