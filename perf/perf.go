package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os/exec"
	"time"
)

type cmdResult struct {
	cmd     string
	runTime time.Duration
}

func runCmdUtil(c chan cmdResult, e chan error, app, arg0, arg1, arg2 string) {
	start := time.Now()
	cmd := exec.Command(app, arg0, arg1, arg2)
	_, err := cmd.Output()
	if err != nil {
		e <- err
		//fmt.Println(err.Error())
		return
	}
	end := time.Now()
	//fmt.Println(string(data))
	elapsed := end.Sub(start)
	//fmt.Printf("Start: %v, End: %v, Elasped: %v\n", start, end, elapsed)
	c <- cmdResult{arg2, elapsed}
}

func runCmd(c chan cmdResult, e chan error, totalRuns int, app, arg0, arg1, arg2 string) {
	for i := 0; i < totalRuns; i++ {
		go runCmdUtil(c, e, app, arg0, arg1, arg2)
	}
}

func main() {

	totalRuns := flag.Int("runs", 10, "Total number of runs")
	file := flag.String("file", "query.sql", "Total number of runs")
	flag.Parse()

	app := "/usr/lib/presto/bin/presto"

	arg0 := "client"
	arg1 := "--execute"

	data, err := ioutil.ReadFile(*file)
	if err != nil {
		fmt.Println("File reading error", err)
		return
	}
	fmt.Printf("\n\nRunning SQL Query: %s for: %d Times: \n\n", string(data), *totalRuns)

	arg2 := string(data)

	c := make(chan cmdResult)
	e := make(chan error)
	runCmd(c, e, *totalRuns, app, arg0, arg1, arg2)
	count := 1
	totalTime := 0.0
	for i := 0; i < *totalRuns; i++ {
		select {
		case runData := <-c:
			fmt.Printf("No: %d, Time (Seconds): %v\n", count, runData.runTime.Seconds())
			totalTime += runData.runTime.Seconds()
			count++
		case err := <-e:
			fmt.Printf(err.Error())
		}
	}
	fmt.Printf("Total Number of Successful Runs: %d, Average Time (Seconds): %f\n", count, totalTime/float64(count))
}
