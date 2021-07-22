package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const earthRadius = float64(6371)
const invalidVelocity = float64(100)
const secsPerHour = 3600
const fixedFare = float64(1.30)
const minimumFare = float64(3.47)

var concurrency = 100
var fareMapMutex sync.RWMutex

var counter = 0

// WorkStruct struct for holding two consequent lines of read file
type WorkStruct struct {
	id        int64
	timestamp int64
	velocity  float64
	distance  float64
	dt        float64
}

// LineStruct struct for holding id, latitude, lognitude and timpestamp of each line of file
type LineStruct struct {
	id        int64
	lat       float64
	lng       float64
	timestamp int64
}

func main() {

	// Put the absolute path of your file
	filePath := "/Users/dimitris/Desktop/beatFareAssignment/paths.csv"

	// Channel as work queue for putting the valid velocity WorkStructs of the lines of the file
	workQueue := make(chan WorkStruct)

	// We need to know when everyone is done so we can exit
	complete := make(chan bool)

	// Map to hold fares per ride
	faresMap := make(map[int64]float64)

	// Filter valid lines and put them into the work queue
	go func() {

		file, e := os.Open(filePath)
		if e != nil {
			log.Fatal(e)
		}
		// Close when the function returns
		defer file.Close()

		var line1 string
		var line2 string
		var validVelocityFlag = false

		scanner := bufio.NewScanner(file)
		scanner.Scan()
		line1 = scanner.Text()

		// Get valid WorkStructs and send them into "workQueue" channel
		for scanner.Scan() {

			if validVelocityFlag == true {
				line1 = line2
			}

			line2 = scanner.Text()

			l1 := makeLineStruct(line1)
			l2 := makeLineStruct(line2)

			if l1.id != l2.id {
				continue
			}

			distance, dt, velocity := calculateMovement(l1, l2)

			if velocity < invalidVelocity {
				validVelocityFlag = true
				workQueue <- WorkStruct{l1.id, l2.timestamp, velocity, distance, dt}
			} else {
				validVelocityFlag = false
			}

		}

		// Close the channel so everyone reading from it knows we're done.
		close(workQueue)
	}()

	// Now read them all off, concurrently.
	for i := 0; i < concurrency; i++ {
		go startWorking(workQueue, complete, faresMap)
	}

	// Wait for everyone to finish.
	for i := 0; i < concurrency; i++ {
		<-complete
	}

	// Create text file to save fare estimates
	file, err := os.Create("fareEstimation.txt")
	if err != nil {
		log.Fatal("Cannot create file", err)
	}
	defer file.Close()

	//Save fare estimates to file
	for key, value := range faresMap {
		if value+fixedFare <= minimumFare {
			faresMap[key] = minimumFare
		} else {
			faresMap[key] = value + fixedFare
		}
		fmt.Fprintf(file, "%d, %.2f\n", key, faresMap[key])
	}
}

// function that starts processing the valid WorkStructs of lines
func startWorking(workStructs <-chan WorkStruct, complete chan<- bool, faresMap map[int64]float64) {

	for workStruct := range workStructs {
		counter++
		calculateFare(workStruct, faresMap)
	}
	complete <- true
}

// function that calculates the actual fare of every segment and adds it to the fares map
func calculateFare(ws WorkStruct, faresMap map[int64]float64) {

	var fare float64

	if ws.velocity > 10 {
		hour := convertTimestampToDate(ws.timestamp).Hour()
		if hour > 5 || hour == 0 {
			fare = 0.74 * ws.distance
		} else {
			fare = 1.30 * ws.distance
		}
	} else {
		fare = 11.9 * ws.dt
	}

	fareMapMutex.Lock()
	faresMap[ws.id] += fare
	fareMapMutex.Unlock()
}

// function to check if velocity from a segment S is valid
func calculateMovement(b1 LineStruct, b2 LineStruct) (float64, float64, float64) {
	velocity := invalidVelocity
	dt := math.Abs((float64)(b1.timestamp-b2.timestamp)) / secsPerHour
	ds := haversineDistance(b2.lng, b2.lat, b1.lng, b1.lat)

	if dt != 0 {
		velocity = ds / dt
	}

	return ds, dt, velocity
}

// function to construct a LineStruct for separating values from line of file
func makeLineStruct(s string) LineStruct {
	splitS := strings.Split(s, ",")
	id, _ := strconv.ParseInt(splitS[0], 10, 64)
	lat, _ := strconv.ParseFloat(splitS[1], 64)
	lng, _ := strconv.ParseFloat(splitS[2], 64)
	timestamp, _ := strconv.ParseInt(splitS[3], 10, 64)

	return LineStruct{id, lat, lng, timestamp}
}

// function to calculate the harvesine distance
func haversineDistance(lonFrom float64, latFrom float64, lonTo float64, latTo float64) (distance float64) {
	var deltaLat = (latTo - latFrom) * (math.Pi / 180)
	var deltaLon = (lonTo - lonFrom) * (math.Pi / 180)

	var a = math.Sin(deltaLat/2)*math.Sin(deltaLat/2) +
		math.Cos(latFrom*(math.Pi/180))*math.Cos(latTo*(math.Pi/180))*
			math.Sin(deltaLon/2)*math.Sin(deltaLon/2)
	var c = 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	distance = earthRadius * c

	return
}

// function to convert timestamp to datetime
func convertTimestampToDate(timestamp int64) time.Time {
	return time.Unix(timestamp, 0)
}
