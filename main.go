package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

func parseLine(line string, dateField, userField regexp.Regexp) (ts time.Time, user string, err error) {
	var (
		w         sync.WaitGroup
		day       int
		year      int
		month     int
		validDate bool
	)

	w.Add(2)
	go func(df regexp.Regexp) {
		validDate = false

		date := df.FindString(line)
		dateParts := strings.Split(date, "/")

		if len(dateParts) > 2 {
			day, err = strconv.Atoi(dateParts[1])
			year, err = strconv.Atoi("20" + dateParts[2])
			month, err = strconv.Atoi(dateParts[0])

			ts = time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.Local)
			validDate = true
		}

		w.Done()
	}(dateField)

	go func(uf regexp.Regexp) {
		user = uf.FindString(line)
		w.Done()
	}(userField)

	w.Wait()

	if validDate {
		return ts, user, err
	}
	return time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local), "", fmt.Errorf("unable to parse date in line %q", line)
}

func main() {
	var (
		inFile             = flag.String("f", "Chatlog.txt", "Input file")
		df                 = flag.String("d", "^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2}", "Date stamp format in chat log (regexp)")
		uf                 = flag.String("u", "[a-zA-Z]+", "User field (regexp, first match)")
		timeseriesEndpoint = flag.String("m", "http://localhost:8086/write?db=chatstat&precision=s", "Timeseries DB endpoint")
		distribution       = map[time.Time]map[string]int{}
	)
	flag.Parse()

	dateField := regexp.MustCompile(*df)
	userField := regexp.MustCompile(*uf)

	f, err := os.Open(*inFile)
	if err != nil {
		fmt.Errorf(err.Error())
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		date, user, err := parseLine(line, *dateField, *userField)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		if _, found := distribution[date]; !found {
			distribution[date] = map[string]int{user: 1}
		} else {
			if _, count := distribution[date][user]; count {
				distribution[date][user]++
			} else {
				distribution[date][user] = 1
			}
		}
	}

	for timestamp, entries := range distribution {
		for user, messageCount := range entries {
			fmt.Printf("curl '%s' --data-binary 'messages,u=%s value=%d %d'\n",
				*timeseriesEndpoint, user, messageCount, timestamp.Unix())
		}
	}
}
