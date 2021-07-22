package main

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestConvertTimestampToDate(t *testing.T) {
	expected := "2017-12-10 14:27:55 +0200 EET"
	actual := convertTimestampToDate(1512908875)

	if actual.String() != expected {
		t.Error("Convert timestamp to date test failed")
	}
}

func TestHaversineDistance(t *testing.T) {
	expected := 0.12067225018354148
	actual := haversineDistance(23.722412, 37.961273, 23.723453, 37.961983)

	if actual != expected {
		t.Error("Haversine distance test failed")
	}
}

func TestMakeLineStruct(t *testing.T) {
	line := "1,37.966195,23.728613,1405595009"
	id, lat, lng, timestamp := "1", "37.966195", "23.728613", "1405595009"
	idInt, _ := strconv.ParseInt(id, 10, 64)
	latInt, _ := strconv.ParseFloat(lat, 64)
	lngInt, _ := strconv.ParseFloat(lng, 64)
	timestampInt, _ := strconv.ParseInt(timestamp, 10, 64)
	actual := LineStruct{idInt, latInt, lngInt, timestampInt}
	expected := makeLineStruct(line)

	if actual != expected {
		t.Error("Make line struct test failed")
	}
}

func TestCalculateMovement(t *testing.T) {

	line1 := "1,37.964968,23.727183,1405595085"
	line2 := "1,37.964982,23.72712,1405595093"
	ls1 := makeLineStruct(line1)
	ls2 := makeLineStruct(line2)
	expected1, expected2, expected3 := 0.005738076073033498, 0.0022222222222222222, 2.582134232865074
	actual1, actual2, actual3 := calculateMovement(ls1, ls2)

	if actual1 != expected1 || actual2 != expected2 || actual3 != expected3 {
		t.Error("Calculate movement test failed")
	}
}

func TestCalculateMovementInvalid(t *testing.T) {

	line1 := "1,37.964968,23.727183,1405595085"
	line2 := "1,37.964982,23.72712,1405595085"
	ls1 := makeLineStruct(line1)
	ls2 := makeLineStruct(line2)
	expected1, expected2, expected3 := 0.005738076073033498, 0.0, 100.0
	actual1, actual2, actual3 := calculateMovement(ls1, ls2)

	if actual1 != expected1 || actual2 != expected2 || actual3 != expected3 {
		t.Error("Calculate movement test failed")
	}
}

func TestCalculateFareVelocityBelow10(t *testing.T) {

	faresMap := make(map[int64]float64)
	line1 := "1,37.964968,23.727183,1405595085"
	line2 := "1,37.964982,23.72712,1405595093"
	ls1 := makeLineStruct(line1)
	ls2 := makeLineStruct(line2)
	distance, dt, velocity := calculateMovement(ls1, ls2)
	ws := WorkStruct{ls2.id, ls2.timestamp, velocity, distance, dt}
	expected := 0.026444444444444444
	calculateFare(ws, faresMap)
	actual := faresMap[ls2.id]

	if actual != expected {
		t.Error("Calculate fare test failed")
	}

}

func TestCalculateFareVelocityAbove10(t *testing.T) {

	faresMap := make(map[int64]float64)
	line1 := "1,37.9645,23.728,1405595085"
	line2 := "1,37.964982,23.72712,1405595093"
	ls1 := makeLineStruct(line1)
	ls2 := makeLineStruct(line2)
	distance, dt, velocity := calculateMovement(ls1, ls2)
	ws := WorkStruct{ls2.id, ls2.timestamp, velocity, distance, dt}
	expected := 0.06951234016754937
	calculateFare(ws, faresMap)
	actual := faresMap[ls2.id]

	if actual != expected {
		t.Error("Calculate fare test failed")
	}
}

func TestCalculateFareVelocityLateHours(t *testing.T) {

	faresMap := make(map[int64]float64)
	line1 := "1,37.9645,23.728,1405558800"
	line2 := "1,37.964982,23.72712,1405558808"
	ls1 := makeLineStruct(line1)
	ls2 := makeLineStruct(line2)
	distance, dt, velocity := calculateMovement(ls1, ls2)
	ws := WorkStruct{ls2.id, ls2.timestamp, velocity, distance, dt}
	expected := 0.12211627326731646
	calculateFare(ws, faresMap)
	actual := faresMap[ls2.id]

	if actual != expected {
		t.Error("Calculate fare test failed")
	}
}

func TestMain(t *testing.T) {

	main()
	filePath := "/Users/dimitris/Desktop/beatFareAssignment/src/fareEstimation.txt"
	file, e := os.Open(filePath)
	if e != nil {
		log.Fatal(e)
	}
	// Close when the function returns
	defer file.Close()
	var splitSlice []string
	scanner := bufio.NewScanner(file)
	scanner.Scan()
	for scanner.Scan() {
		splitSlice = strings.Split(scanner.Text(), ", ")
		switch id := splitSlice[0]; id {
		case "1":
			if splitSlice[1] != "11.34" {
				t.Error("Fare for id 1 is not correct")
			}
		case "2":
			if splitSlice[1] != "13.10" {
				t.Error("Fare for id 2 is not correct")
			}
		case "3":
			if splitSlice[1] != "33.84" {
				t.Error("Fare for id 3 is not correct")
			}
		case "4":
			if splitSlice[1] != "3.47" {
				t.Error("Fare for id 4 is not correct")
			}
		case "5":
			if splitSlice[1] != "22.78" {
				t.Error("Fare for id 5 is not correct")
			}
		case "6":
			if splitSlice[1] != "9.41" {
				t.Error("Fare for id 6 is not correct")
			}
		case "7":
			if splitSlice[1] != "30.01" {
				t.Error("Fare for id 7 is not correct")
			}
		case "8":
			if splitSlice[1] != "9.21" {
				t.Error("Fare for id 8 is not correct")
			}
		case "9":
			if splitSlice[1] != "6.35" {
				t.Error("Fare for id 9 is not correct")
			}
		default:

		}
	}
}
