package main

import (
	"bufio"
	"log"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Info struct {
	Min   float64
	Max   float64
	Sum   float64
	Count int
	Avg   float64
}

var weatherData = make(map[string]*Info)
var count int
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

	process()

	weatherStationNames := make([]string, 0, len(weatherData))
	for key, value := range weatherData {
		average := value.Sum / float64(value.Count)
		value.Avg = average
		weatherData[key] = value

		weatherStationNames = append(weatherStationNames, key)
	}

	sort.Strings(weatherStationNames)

	for _, weatherStation := range weatherStationNames {
		info := weatherData[weatherStation]
		log.Println(weatherStation, "Min:", info.Min, "Max:", info.Max, "Avg:", info.Avg)
	}

	log.Println("Elapsed time:", time.Since(start))
}

func process() {
	file, err := os.Open("measurements.txt")
	if err != nil {
		log.Fatalf("failed opening file: %s", err)
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		weatherStation, strTemperature, _ := strings.Cut(line, ";")

		temperature, err := strconv.ParseFloat(strTemperature, 64)
		if err != nil {
			log.Fatalf("failed parsing temperature: %s", err)
		}

		val, ok := weatherData[weatherStation]
		if !ok {
			weatherData[weatherStation] = &Info{
				Min:   temperature,
				Max:   temperature,
				Sum:   temperature,
				Count: 1,
			}
		} else {

			if temperature < val.Min {
				val.Min = temperature
			}
			if temperature > val.Max {
				val.Max = temperature
			}
			val.Sum += temperature
			val.Count++
		}

		//count++
		//if count%1000000 == 0 {
		//	log.Println(count)
		//	since := time.Since(now).Seconds()
		//	ratio := float64(1000000000 / 1000000)
		//	estimated := since * ratio / 60
		//	log.Println("Estimated time:", estimated, " minutes")
		//	now = time.Now()
		//}
	}

}
