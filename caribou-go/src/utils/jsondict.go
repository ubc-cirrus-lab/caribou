package utils

import "github.com/tidwall/gjson"

type JsonDict struct {
	JsonString string
}

func (d *JsonDict) GetString(key string, defaultValue string) string {
	result := gjson.Get(d.JsonString, key)
	if result.Exists() {
		return result.String()
	} else {
		return defaultValue
	}
}

func (d *JsonDict) GetNumeric(key string, defaultValue float64) float64 {
	result := gjson.Get(d.JsonString, key)
	if result.Exists() {
		return result.Float()
	} else {
		return defaultValue
	}
}

func resultListToNumericList(resultArray []gjson.Result) []float64 {
	resultFloat := make([]float64, len(resultArray))
	for i, n := range resultArray {
		resultFloat[i] = n.Float()
	}
	return resultFloat
}

func (d *JsonDict) GetNumericList(key string, defaultValue []float64) []float64 {
	result := gjson.Get(d.JsonString, key)
	resultArray := result.Array()
	if result.IsArray() {
		return resultListToNumericList(resultArray)
	} else {
		return defaultValue
	}
}

func (d *JsonDict) GetDict(key string, defaultValue map[string]JsonDict) map[string]JsonDict {
	result := gjson.Get(d.JsonString, key)
	returnResult := make(map[string]JsonDict)
	if result.Exists() {
		for key, value := range result.Map() {
			returnResult[key] = JsonDict{value.Str}
		}
		return returnResult
	} else {
		return defaultValue
	}
}

func (d *JsonDict) GetStringNumericListMap(key string, defaultValue map[string][]float64) map[string][]float64 {
	result := gjson.Get(d.JsonString, key)
	returnResult := make(map[string][]float64)
	if result.Exists() {
		for key, value := range result.Map() {
			returnResult[key] = resultListToNumericList(value.Array())
		}
		return returnResult
	} else {
		return defaultValue
	}
}
