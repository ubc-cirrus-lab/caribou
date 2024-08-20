package utils

import (
	"testing"
)

func TestGet(t *testing.T) {
	tests := []struct {
		dict         map[string]interface{}
		defaultValue interface{}
		keys         []string
		expected     interface{}
	}{
		{
			dict: map[string]interface{}{
				"a": "apple",
				"b": map[string]interface{}{
					"c": "cat",
				},
			},
			defaultValue: "default",
			keys:         []string{"a"},
			expected:     "apple",
		},
		{
			dict: map[string]interface{}{
				"a": "apple",
				"b": map[string]interface{}{
					"c": "cat",
				},
			},
			defaultValue: "default",
			keys:         []string{"b", "c"},
			expected:     "cat",
		},
		{
			dict: map[string]interface{}{
				"a": "apple",
				"b": map[string]interface{}{
					"c": "cat",
				},
			},
			defaultValue: "default",
			keys:         []string{"b", "d"},
			expected:     "default",
		},
	}

	for _, test := range tests {
		result := Get(test.dict, test.defaultValue, test.keys...)
		if result != test.expected {
			t.Errorf("Get(%v, %v, %v) = %v; want %v", test.dict, test.defaultValue, test.keys, result, test.expected)
		}
	}
}

func TestGet_Map_string_interface(t *testing.T) {
	tests := []struct {
		dict         map[string]interface{}
		defaultValue map[string]interface{}
		keys         []string
		expected     map[string]interface{}
	}{
		{
			dict: map[string]interface{}{
				"a": "apple",
				"b": map[string]interface{}{
					"c": map[string]string{
						"d": "dog",
					},
				},
			},
			defaultValue: map[string]interface{}{},
			keys:         []string{"b", "c"},
			expected: map[string]interface{}{
				"d": "dog",
			},
		},
	}

	for _, test := range tests {
		result := Get(test.dict, test.defaultValue, test.keys...)
		for k, v := range result {
			if v != test.expected[k] {
				t.Errorf("Get(%v, %v, %v) = %v; want %v", test.dict, test.defaultValue, test.keys, result, test.expected)
			}
		}

	}
}

func TestGet_Map_string_float(t *testing.T) {
	tests := []struct {
		dict         map[string]interface{}
		defaultValue map[string]float64
		keys         []string
		expected     map[string]float64
	}{
		{
			dict: map[string]interface{}{
				"a": "apple",
				"b": map[string]interface{}{
					"c": map[string]string{
						"d": "dog",
					},
					"d": map[string]float64{
						"d1": 1.1,
						"d2": 2.2,
					},
				},
			},
			defaultValue: map[string]float64{},
			keys:         []string{"b", "d"},
			expected: map[string]float64{
				"d1": 1.1,
				"d2": 2.2,
			},
		},
	}

	for _, test := range tests {
		result := Get(test.dict, test.defaultValue, test.keys...)
		for k, v := range result {
			if v != test.expected[k] {
				t.Errorf("Get(%v, %v, %v) = %v; want %v", test.dict, test.defaultValue, test.keys, result, test.expected)
			}
		}

	}
}

func TestGet_Map_string_int(t *testing.T) {
	tests := []struct {
		dict         map[string]interface{}
		defaultValue map[string]int
		keys         []string
		expected     map[string]int
	}{
		{
			dict: map[string]interface{}{
				"a": "apple",
				"b": map[string]interface{}{
					"c": map[string]string{
						"d": "dog",
					},
					"d": map[string]int{
						"d1": 1,
						"d2": 2,
					},
				},
			},
			defaultValue: map[string]int{},
			keys:         []string{"b", "d"},
			expected: map[string]int{
				"d1": 1,
				"d2": 2,
			},
		},
	}

	for _, test := range tests {
		result := Get(test.dict, test.defaultValue, test.keys...)
		for k, v := range result {
			if v != test.expected[k] {
				t.Errorf("Get(%v, %v, %v) = %v; want %v", test.dict, test.defaultValue, test.keys, result, test.expected)
			}
		}

	}
}

func TestGetList(t *testing.T) {
	tests := []struct {
		dict         map[string]interface{}
		defaultValue []string
		keys         []string
		expected     []string
	}{
		{
			dict: map[string]interface{}{
				"a": []interface{}{"apple", "apricot"},
				"b": map[string]interface{}{
					"c": []interface{}{"cat", "car"},
				},
			},
			defaultValue: []string{"default"},
			keys:         []string{"a"},
			expected:     []string{"apple", "apricot"},
		},
		{
			dict: map[string]interface{}{
				"a": []interface{}{"apple", "apricot"},
				"b": map[string]interface{}{
					"c": []interface{}{"cat", "car"},
				},
			},
			defaultValue: []string{"default"},
			keys:         []string{"b", "c"},
			expected:     []string{"cat", "car"},
		},
		{
			dict: map[string]interface{}{
				"a": []interface{}{"apple", "apricot"},
				"b": map[string]interface{}{
					"c": []interface{}{"cat", "car"},
				},
			},
			defaultValue: []string{"default"},
			keys:         []string{"b", "d"},
			expected:     []string{"default"},
		},
		{
			dict: map[string]interface{}{
				"a": "not a list",
				"b": map[string]interface{}{
					"c": []interface{}{"cat", "car"},
				},
			},
			defaultValue: []string{"default"},
			keys:         []string{"a"},
			expected:     []string{"default"},
		},
	}

	for _, test := range tests {
		result := GetList(test.dict, test.defaultValue, test.keys...)
		if len(result) != len(test.expected) {
			t.Errorf("GetList(%v, %v, %v) = %v; want %v", test.dict, test.defaultValue, test.keys, result, test.expected)
		}
		for i := range result {
			if result[i] != test.expected[i] {
				t.Errorf("GetList(%v, %v, %v) = %v; want %v", test.dict, test.defaultValue, test.keys, result, test.expected)
				break
			}
		}
	}
}

func TestGetList_Nested(t *testing.T) {
	tests := []struct {
		dict         map[string]interface{}
		defaultValue [][]string
		keys         []string
		expected     [][]string
	}{
		{
			dict: map[string]interface{}{
				"a": []interface{}{"apple", "apricot"},
				"b": map[string]interface{}{
					"c": []interface{}{"cat", "car"},
					"d": [][]string{{"A", "B"}, {"C", "D", "E"}},
				},
			},
			defaultValue: [][]string{{"default"}},
			keys:         []string{"b", "d"},
			expected:     [][]string{{"A", "B"}, {"C", "D", "E"}},
		},
	}

	for _, test := range tests {
		result := GetList(test.dict, test.defaultValue, test.keys...)
		if len(result) != len(test.expected) {
			t.Errorf("GetList(%v, %v, %v) = %v; want %v", test.dict, test.defaultValue, test.keys, result, test.expected)
		}
		for i := range result {
			if len(result[i]) != len(test.expected[i]) {
				t.Errorf("GetList(%v, %v, %v) = %v; want %v", test.dict, test.defaultValue, test.keys, result, test.expected)
			}
			for j := range result[i] {
				if result[i][j] != test.expected[i][j] {
					t.Errorf("GetList(%v, %v, %v) = %v; want %v", test.dict, test.defaultValue, test.keys, result, test.expected)
					break
				}
			}
		}
	}
}
