package main

import (
	"C"
	"bufio"
	"encoding/json"
	"math/rand"
	"os"
	"reflect"

	deploymentmetricscalculator "caribou-go/src/deployment-metrics-calculator"
	"caribou-go/src/utils"
)

// TODO: Make this path dynamic
const (
	maxCapacity      = 512 * 1024
	datapipefileSend = "/Users/pjavanrood/Documents/Code/caribou-go/data_go_py"
	datapipefileRec  = "/Users/pjavanrood/Documents/Code/caribou-go/data_py_go"
)

var (
	signalChan  chan struct{}
	mainRunning bool
)

func parseInputData(byteValue *[]byte) (command string, data string, err error) {
	var inputJson map[string]interface{}
	err = json.Unmarshal(*byteValue, &inputJson)
	if err != nil {
		return "", "", err
	}
	command = utils.Get(inputJson, "", "command")
	if len(command) == 0 {
		return "", "", err
	}
	data = utils.Get(inputJson, "", "data")
	return command, data, nil
}

func sendData[T any](toSend T) {
	datapipeSend, errS := os.OpenFile(datapipefileSend, os.O_WRONLY, os.ModeNamedPipe)
	defer func(datapipeSend *os.File) {
		err := datapipeSend.Close()
		if err != nil {
		}
	}(datapipeSend)
	if errS != nil {
		return
	}
	toSendJson, _ := json.Marshal(map[string]interface{}{"data": toSend})
	_, _ = datapipeSend.Write(toSendJson)
}

func receiveData() ([]byte, error) {
	datapipeRec, errR := os.OpenFile(datapipefileRec, os.O_RDONLY, os.ModeNamedPipe)
	defer func(datapipeRec *os.File) {
		err := datapipeRec.Close()
		if err != nil {
		}
	}(datapipeRec)
	if errR != nil {
		return nil, errR
	}
	scanner := bufio.NewScanner(datapipeRec)
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)
	if scanner.Scan() {
		return scanner.Bytes(), nil
	}
	return nil, scanner.Err()
}

func handleInput() {
	var metricsCalculator *deploymentmetricscalculator.SimpleDeploymentMetricsCalculator
	for {
		if !mainRunning {
			<-signalChan
		}
		buf, err := receiveData()
		if err != nil {
			sendData("nil")
			continue
		}
		command, data, err := parseInputData(&buf)
		if err != nil {
			continue
		}
		if command == "Setup" {
			metricsCalculator = deploymentmetricscalculator.SetupSimpleDeploymentMetricsCalculator(data)
			sendData("void")
		} else {
			commandMethod := reflect.ValueOf(metricsCalculator).MethodByName(command)
			if commandMethod.IsValid() {
				result := commandMethod.Call([]reflect.Value{reflect.ValueOf(data)})
				var toSend interface{}
				if len(result) > 0 {
					toSend = result[0].Interface()
				} else {
					toSend = "void"
				}
				sendData(toSend)
			} else {
				sendData("nil")
			}
		}
	}
}

func run() {
	handleInput()
}

//export start
func start() {
	mainRunning = false
	signalChan = make(chan struct{})
	go run()
}

//export goRead
func goRead() {
	signalChan <- struct{}{}
	return
}

func main() {
	rand.Seed(0)
	mainRunning = true
	run()
}
