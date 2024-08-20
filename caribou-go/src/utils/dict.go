package utils

import (
	"reflect"
)

func Get[V comparable, T any](dict map[V]interface{}, defaultValue T, keys ...V) T {
	var value interface{} = dict

	for _, key := range keys {
		mapValue, ok := value.(map[V]interface{})
		if !ok {
			return defaultValue
		}
		if value, ok = mapValue[key]; !ok {
			return defaultValue
		}
	}

	valueType := reflect.TypeOf(defaultValue)
	valueKind := valueType.Kind()

	switch valueKind {
	case reflect.Map:
		if reflect.TypeOf(value).Kind() == reflect.Map {
			result := reflect.MakeMap(valueType)
			for _, k := range reflect.ValueOf(value).MapKeys() {
				toSet := reflect.ValueOf(value).MapIndex(k)
				switch reflect.ValueOf(value).MapIndex(k).Interface().(type) {
				case int:
					if valueType.Elem().Kind() == reflect.Float64 {
						toSet = reflect.ValueOf(float64(toSet.Interface().(int)))
					} else {
						toSet = reflect.ValueOf(toSet.Interface().(int))
					}
				case float64:
					if valueType.Elem().Kind() == reflect.Int {
						toSet = reflect.ValueOf(int(toSet.Interface().(float64)))
					} else {
						toSet = reflect.ValueOf(toSet.Interface().(float64))
					}
				default:
				}

				result.SetMapIndex(k, toSet)
			}
			return result.Interface().(T)
		}
	case reflect.Slice:
		if reflect.TypeOf(value).Kind() == reflect.Slice {
			sliceValue := reflect.ValueOf(value)
			result := reflect.MakeSlice(valueType, sliceValue.Len(), sliceValue.Cap())
			for i := 0; i < sliceValue.Len(); i++ {
				result.Index(i).Set(sliceValue.Index(i))
			}
			return result.Interface().(T)
		}
	default:
		return value.(T)
	}

	return defaultValue
}

func GetList[V comparable, T any](dict map[V]interface{}, defaultValue []T, keys ...V) []T {
	var value interface{} = dict
	for _, key := range keys {
		mapValue, ok := value.(map[V]interface{})
		if !ok {
			return defaultValue
		}
		if value, ok = mapValue[key]; !ok {
			return defaultValue
		}
	}
	valueType := reflect.TypeOf(defaultValue)
	if valueType.Kind() != reflect.Slice {
		return defaultValue
	}
	if reflect.TypeOf(value).Kind() != reflect.Slice {
		return defaultValue
	}
	interfaceSlice := reflect.ValueOf(value)
	result := reflect.MakeSlice(valueType, interfaceSlice.Len(), interfaceSlice.Cap())

	for i := 0; i < interfaceSlice.Len(); i++ {
		elem := interfaceSlice.Index(i).Interface()
		if reflect.TypeOf(elem).Kind() == reflect.Slice {
			nestedSlice := reflect.MakeSlice(valueType.Elem(), reflect.ValueOf(elem).Len(), reflect.ValueOf(elem).Cap())
			for j := 0; j < reflect.ValueOf(elem).Len(); j++ {
				toSet := reflect.ValueOf(elem).Index(j)
				switch reflect.ValueOf(elem).Index(j).Interface().(type) {
				case int:
					if valueType.Elem().Kind() == reflect.Float64 {
						toSet = reflect.ValueOf(float64(toSet.Interface().(int)))
					} else {
						toSet = reflect.ValueOf(toSet.Interface().(int))
					}
				case float64:
					if valueType.Elem().Kind() == reflect.Int {
						toSet = reflect.ValueOf(int(toSet.Interface().(float64)))
					} else {
						toSet = reflect.ValueOf(toSet.Interface().(float64))
					}
				default:
				}

				nestedSlice.Index(j).Set(toSet)
			}
			result.Index(i).Set(nestedSlice)
		} else {
			result.Index(i).Set(reflect.ValueOf(elem))
		}
	}

	return result.Interface().([]T)
}
