/*
The task is to write a program which reads the file, calculates the min, mean, and max temperature value per weather station,
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

// Optimisation: Reusing buffers after having been parsed reduces memory allocation from
// approx 30,000 buffers of 1024 x 512kb to approx 10,000
// Reduced memory allocation from ~15gb to ~5gb
var BufferPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, BUFFER_SIZE)
	},
}

const (
	BUFFER_SIZE = 1024 * 512
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

type Tally struct {
	results map[string]*StationResult
}

func (t *Tally) Print() {
	for k, v := range t.results {
		fmt.Println("result", string(string(k)), float32(v.min)/10, float32(v.max)/10, float32(v.sum)/10/float32(v.count))
	}
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
	filePtr, err := os.Open("./test_measurements.txt")

	if err != nil {
		log.Fatal("Error reading file")
	}

	start := time.Now()

	//Optimisation: Multithreading application.
	//Use channels to synchronise
	linesCh := readInFile(filePtr)
	out := parseCh(linesCh)
	<-out

	//FinalTally.Print()

	//Timing
	elapsed := time.Since(start)
	fmt.Println(elapsed)

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
			if b == byte(';') {
				semiColonIdx = i
			}
		}

		if semiColonIdx == -1 {
			continue
		}

		station := b[0:semiColonIdx]

		stationTemp := 0

		//Optimisation: parse backwards over the float and do integer arithmetic to avoid floating point arithmetic
		//Stored values are multiplied by 10 to remove the one guaranteed decimal point
		power10 := 1

		for i := len(b) - 1; i > semiColonIdx; i-- {
			if b[i] == byte('.') {
				continue
			}

			if b[i] == byte('-') {
				stationTemp *= -1
				break
			}

			stationTemp += int(b[i]-48) * power10
			power10 *= 10
		}

		//TODO: fix race condition
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

	//Return buffer to pool
	BufferPool.Put(chunk)
}

func readInFile(filePtr *os.File) <-chan []byte {
	//Optimisation: Read into a single buffer, clone the results into a channel
	//Works best with approx 512kb x 512kb buffer size
	buffer := make([]byte, BUFFER_SIZE)
	fragment := make([]byte, 0)

	//fragLength is the value returned by copy otherwise I would just do len(fragment)
	fragLength := 0
	out := make(chan []byte)

	go func() {
		for {
			//Buffer only gets returned to the pool when a scanner has read all it's bytes
			clone := BufferPool.Get().([]byte)

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

			//Here the number of bytes in the buffer is fragLength + bytes copied.
			//Optimisation: Read backwards over the partial line and copy it into the fragment buffer for use next time through.
			if buffer[(fragLength+n)-1] != byte('\n') {
				for i := n - 1; i >= 0; i-- {
					if buffer[i] == byte('\n') {
						copy(fragment, buffer[i:])
						n = i
						break
					}
				}
			}

			//Reslice the pool buffer to be the length of bytes read + fragment length
			//Copy the buffer content into a new buffer up until the beginning of the fragmented line which is now stored for next iteration
			clone = clone[0:n]
			copy(clone, buffer[:n])

			out <- clone
		}
		close(out)
	}()
	return out
}
