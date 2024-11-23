package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Info struct {
	Min   float64
	Max   float64
	Sum   float64
	Count int
	Avg   float64
}

var weatherData = make(map[string]Info)
var mapMutex sync.Mutex

var count int
var countMux sync.Mutex

var wg sync.WaitGroup

var now = time.Now()

func main() {
	f, err := os.Create("cpu_profile.prof")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	defer f.Close()
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}
	defer pprof.StopCPUProfile()

	start := time.Now()

	//importFile()
	process()

	weatherStationNames := make([]string, 0, len(weatherData))
	for key, value := range weatherData {
		average := value.Sum / float64(value.Count)
		value.Avg = average
		weatherData[key] = value

		weatherStationNames = append(weatherStationNames, key)
	}

	sort.Strings(weatherStationNames)

	var sortedWeatherData = make(map[string]Info)
	for _, name := range weatherStationNames {
		sortedWeatherData[name] = weatherData[name]
	}

	fmt.Println("Elapsed time:", time.Since(start))

	err = writeToCSV(sortedWeatherData)
	if err != nil {
		log.Fatalf("failed to write to CSV: %s", err)
	}

	log.Println("Done")
}

func process() {
	file, err := os.Open("measurements.txt")
	if err != nil {
		panic(err)
	}

	//var recordsChannel = make(chan []string)

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		records := strings.Split(line, ";")
		//recordsChannel <- records
		worker(records)
	}

	//go func() {
	//	defer close(recordsChannel)
	//	for scanner.Scan() {
	//		line := scanner.Text()
	//		records := strings.Split(line, ";")
	//		recordsChannel <- records
	//	}
	//}()
	//
	//for i := 0; i < 10; i++ {
	//	go func() {
	//		wg.Add(1)
	//		defer wg.Done()
	//		for record := range recordsChannel {
	//			worker(record)
	//		}
	//	}()
	//}
	//
	//time.Sleep(1 * time.Second)
	//
	//wg.Wait()

	//const bufferSize = 1024
	//buffer := make([]byte, bufferSize)
	//
	//var rawBytesChannel = make(chan []byte)
	//
	//for {
	//	bytesRead, err := file.Read(buffer)
	//	if err != nil {
	//		if err == io.EOF {
	//			log.Println("EOF")
	//			break
	//		}
	//		panic(err)
	//	}
	//	rawBytesChannel <- buffer[:bytesRead]
	//}
	//
	//for rawData := range rawBytesChannel {
	//	_ = rawData
	//}

}

func writeToCSV(data map[string]Info) error {
	file, err := os.Create("weather_data.csv")
	if err != nil {
		return fmt.Errorf("failed to create result file: %s", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	err = writer.Write([]string{"City", "Min", "Max", "Sum", "Count", "Avg"})
	if err != nil {
		return fmt.Errorf("failed to write header: %s", err)
	}

	for city, info := range data {
		record := []string{
			city,
			strconv.FormatFloat(info.Min, 'f', 2, 64),
			strconv.FormatFloat(info.Max, 'f', 2, 64),
			strconv.FormatFloat(info.Sum, 'f', 2, 64),
			strconv.Itoa(info.Count),
			strconv.FormatFloat(info.Avg, 'f', 2, 64),
		}
		err := writer.Write(record)
		if err != nil {
			return fmt.Errorf("failed to write record: %s", err)
		}
	}

	return nil
}

func worker(record []string) {
	temperature, err := strconv.ParseFloat(record[1], 64)
	if err != nil {
		panic(err)
	}

	mapMutex.Lock()
	val, ok := weatherData[record[0]]
	if !ok {
		val = Info{
			Min:   temperature,
			Max:   temperature,
			Sum:   temperature,
			Count: 1,
		}
	} else {
		info := val
		if temperature < info.Min {
			info.Min = temperature
		}
		if temperature > info.Max {
			info.Max = temperature
		}
		info.Sum += temperature
		info.Count++
		val = info
	}

	weatherData[record[0]] = val
	mapMutex.Unlock()

	countMux.Lock()
	count++
	if count%1000000 == 0 {
		log.Println(count)
		since := time.Since(now).Seconds()
		ratio := float64(1000000000 / 1000000)
		estimated := since * ratio / 60
		log.Println("Estimated time:", estimated, " minutes")
		now = time.Now()
	}
	countMux.Unlock()
}

//
//func importFile() {
//	file, err := os.Open("measurements.txt")
//	if err != nil {
//		panic(err)
//	}
//
//	var rawBytesChannel = make(chan []byte)
//	var linesChannel = make(chan []string)
//
//	var count int
//
//	go func() {
//		readFile(file, rawBytesChannel)
//	}()
//
//	go func() {
//		convertChunksToRecords(linesChannel, rawBytesChannel)
//	}()
//
//	for record := range linesChannel {
//		count++
//		if count%10000000 == 0 {
//			fmt.Println(count)
//		}
//
//		_ = record
//	}
//
//	time.Sleep(1 * time.Second)
//}
//
//func convertChunksToRecords(linesChannel chan []string, rawBytesChannel chan []byte) {
//	defer close(linesChannel)
//	var leftover []byte
//
//	for rawData := range rawBytesChannel {
//		rawData = append(leftover, rawData...)
//		parts := bytes.Split(rawData, []byte("\n"))
//		for i, part := range parts {
//			if i == len(parts)-1 {
//				leftover = part
//			} else {
//				line := string(part)
//				records := strings.Split(line, ";")
//				linesChannel <- records
//			}
//		}
//
//	}
//
//}
//
//func readFile(file *os.File, rawBytesChannel chan []byte) {
//	const bufferSize = 67108864
//	buffer := make([]byte, bufferSize)
//
//	defer close(rawBytesChannel)
//	for {
//		bytesRead, err := file.Read(buffer)
//		if err != nil {
//			if err == io.EOF {
//				break
//			}
//			panic(err)
//		}
//		rawBytesChannel <- buffer[:bytesRead]
//	}
//	log.Printf("File read complete")
//}
