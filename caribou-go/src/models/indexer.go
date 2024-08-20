package models

import (
	"strconv"
)

type Indexer struct {
	valueIndices  map[string]int
	indicesValues map[int]string
}

func (in *Indexer) getValueIndices() map[string]int {
	return in.valueIndices
}

func indicesToValues(valueIndices map[string]int) map[int]string {
	var result map[int]string
	for instance, index := range valueIndices {
		result[index] = instance
	}
	return result
}

func (in *Indexer) ValueToIndex(value string) int {
	return in.valueIndices[value]
}

func (in *Indexer) IndexToValue(index int) string {
	return in.indicesValues[index]
}

type RegionIndexer struct {
	Indexer
}

func NewRegionIndexer(regions []interface{}) *RegionIndexer {
	var valueIndices map[string]int
	for index, region := range regions {
		valueIndices[region.(string)] = index
	}
	re := RegionIndexer{Indexer{
		valueIndices:  valueIndices,
		indicesValues: indicesToValues(valueIndices),
	}}
	return &re
}

type InstanceIndexer struct {
	Indexer
}

func NewInstanceIndexer(nodes []interface{}) *InstanceIndexer {
	var valueIndices map[string]int
	for index, node := range nodes {
		nodeMap := node.(map[string]interface{})
		valueIndices[nodeMap["instance_name"].(string)] = index
	}
	re := InstanceIndexer{Indexer{
		valueIndices:  valueIndices,
		indicesValues: indicesToValues(valueIndices),
	}}
	return &re
}

func parseIndexer(indexerData map[string]interface{}) Indexer {
	valueIndices := map[string]int{}
	for k, v := range indexerData["value_indices"].(map[string]interface{}) {
		valueIndices[k] = int(v.(float64))
	}
	indicesValues := map[int]string{}
	for k, v := range indexerData["indices_to_values"].(map[string]interface{}) {
		kInt, _ := strconv.Atoi(k)
		indicesValues[kInt] = v.(string)
	}
	return Indexer{
		valueIndices:  valueIndices,
		indicesValues: indicesValues,
	}
}

func CopyInstanceIndexer(indexerData map[string]interface{}) *InstanceIndexer {
	return &InstanceIndexer{
		parseIndexer(indexerData),
	}
}

func CopyRegionIndexer(indexerData map[string]interface{}) *RegionIndexer {
	return &RegionIndexer{
		parseIndexer(indexerData),
	}
}
