package main

import (
	"bufio"
	"bytes"
	"log"
	"os"
	"runtime/pprof"
	"sort"
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
}

func process() {
	file, err := os.Open("measurements.txt")
	if err != nil {
		log.Fatalf("failed opening file: %s", err)
	}

	reader := bufio.NewReader(file)
	buffer := make([]byte, 4096*4096)
	var remaining []byte

	for {
		byteAmount, err := reader.Read(buffer)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}

			log.Fatalf("failed reading file: %s", err)
		}

		chunk := buffer[:byteAmount]

		//Only needed because the file was created on Windows
		test := bytes.Replace(chunk, []byte("\r"), []byte(""), -1)
		lines := bytes.Split(test, []byte("\n"))

		for index, line := range lines {
			if index == len(lines)-1 {
				remaining = line
				continue
			}

			if len(remaining) > 0 {
				temp := remaining
				line = append(temp, line...)
				remaining = nil
			}

			byteWeatherStation, byteTemperature, _ := bytes.Cut(line, []byte(";"))

			temperature := convertFloatFromBytes(byteTemperature)
			weatherStation := string(byteWeatherStation)

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

}

// Accuracy only to 1 decimal place and only to 100
func convertFloatFromBytes(bytes []byte) float64 {
	negative := false
	index := 0

	// Check if negative
	if bytes[0] == '-' {
		index++
		negative = true
	}

	value := float64(bytes[index] - '0')
	index++
	if bytes[index] != '.' {
		value = value*10 + float64(bytes[index]-'0')
		index++
	}

	index++
	value += float64(bytes[index]-'0') / 10
	if negative {
		value = -value
	}

	return value
}
