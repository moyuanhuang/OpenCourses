package mapreduce

import (
	"encoding/json"
	"os"
	"io"
	"sort"
)

type ByKey []KeyValue

func (a ByKey) Len() int { return len(a) }
func (a ByKey) Swap(i, j int)  { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFileName string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	outFile, err := os.OpenFile(outFileName, os.O_WRONLY | os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	enc := json.NewEncoder(outFile)
	defer outFile.Close()

	kvMap := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		tempFileName := reduceName(jobName, i, reduceTask)
		tempFile, err := os.OpenFile(tempFileName, os.O_RDONLY, 0600)
		if err != nil {
			panic(err)
		}

		dec := json.NewDecoder(tempFile)
		var kv KeyValue
		for {
			if err := dec.Decode(&kv); err == io.EOF {
				tempFile.Close()
				break
			} else if err != nil {
				panic(err)
			}
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}

	// sort keys
	var sortedKeys = make([]string, 0)
	for k, _ := range kvMap {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	// run reduceF() and output it to outFile
	for _, key := range sortedKeys {
		reducedValue := reduceF(key, kvMap[key])
		enc.Encode(KeyValue{Key: key, Value: reducedValue})
	}
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
}
