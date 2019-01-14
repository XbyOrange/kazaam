package transform

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Shift moves values from one provided json path to another in raw []byte.
func Shift(spec *Config, data []byte) ([]byte, error) {
	var outData []byte
	if spec.InPlace {
		outData = data
	} else {
		outData = []byte(`{}`)
	}
	for k, v := range *spec.Spec {
		array := true
		var keyList []string

		// check if `v` is a string or list and build a list of keys to evaluate
		switch v.(type) {
		case string:
			keyList = append(keyList, v.(string))
			array = false
		case []interface{}:
			for _, vItem := range v.([]interface{}) {
				vItemStr, found := vItem.(string)
				if !found {
					return nil, ParseError(fmt.Sprintf("Warn: Unable to coerce element to json string: %v", vItem))
				}
				keyList = append(keyList, vItemStr)
			}
		default:
			return nil, ParseError(fmt.Sprintf("Warn: Unknown type in message for key: %s", k))
		}

		// iterate over keys to evaluate
		// Note: this could be sped up significantly (especially for large shift transforms)
		// by using `jsonparser.EachKey()` to iterate through data once and pick up all the
		// needed underlying data. It would be a non-trivial update since you'd have to make
		// recursive calls and keep track of all the key paths at each level.
		// Currently we iterate at worst once per key in spec, with a better design it would be once
		// per spec.
		for _, v := range keyList {

			//Special case, N:N array copy, can't be handled by normal value insertion, we need to create  a lot of manual insertions like [0]:[0] [1]:[1],...
			if strings.Contains(v, "[*]") && strings.Count(v, "[*]") == strings.Count(k, "[*]") {
				//now in recursive way  we use the length for each N to replace it and do copy
				outData, _ = insertArrayDataRecursively("", strings.Split(v, "."), 0, v, k, data, spec, array, outData)

			} else {
				return insertShiftData(v, k, data, spec, array, outData)

			}

		}
	}
	return outData, nil
}

func insertArrayDataRecursively(lookPath string, replacementArray []string, level int, v, k string, data []byte, spec *Config, array bool, outData []byte) ([]byte, error) {

	var err error
	originalK := k
	originalV := v

	if level != 0 {
		lookPath += "."
	}
	lookPath += replacementArray[level]
	var tempArray []interface{}
	rawTempJson, err := getJSONRaw(data, lookPath, false)
	if err != nil {
		return nil, err
	}
	json.Unmarshal(rawTempJson, &tempArray)
	totalToIterate := len(tempArray)

	if totalToIterate == 0 && level < (len(replacementArray)-1) {
		//goto next level directly
		outData, _ = insertArrayDataRecursively(lookPath, replacementArray, level+1, v, k, data, spec, array, outData)
	}

	for i := 0; i < totalToIterate; i++ {
		k = strings.Replace(originalK, "[*]", "["+strconv.Itoa(i)+"]", 1)
		v = strings.Replace(originalV, "[*]", "["+strconv.Itoa(i)+"]", 1)
		if level == (len(replacementArray)-1) || !strings.Contains(v, "[*]") {
			outData, err = insertShiftData(v, k, data, spec, array, outData)
		} else {
			nextLevelLookPath := strings.Replace(lookPath, "[*]", "["+strconv.Itoa(i)+"]", 1)
			outData, err = insertArrayDataRecursively(nextLevelLookPath, replacementArray, level+1, v, k, data, spec, array, outData)
		}
	}
	return outData, err

}

func insertShiftData(v, k string, data []byte, spec *Config, array bool, outData []byte) ([]byte, error) {

	var dataForV []byte
	var err error

	// grab the data
	if v == "$" {
		dataForV = data
	} else {
		dataForV, err = getJSONRaw(data, v, spec.Require)
		if err != nil {
			return nil, err
		}
	}

	// if array flag set, encapsulate data
	if array {
		// bookend() is destructive to underlying slice, need to copy.
		// extra capacity saves an allocation and copy during bookend.
		tmp := make([]byte, len(dataForV), len(dataForV)+2)
		copy(tmp, dataForV)
		dataForV = bookend(tmp, '[', ']')
	}
	// Note: following pattern from current Shift() - if multiple elements are included in an array,
	// they will each successively overwrite each other and only the last element will be included
	// in the transformed data.
	outData, err = setJSONRaw(outData, dataForV, k)
	if err != nil {
		return nil, err
	}
	return outData, nil

}
