package mapreduce

import (
	"encoding/csv"
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
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

	out, err := os.OpenFile(outFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Close()

	records := make([]KeyValue, 0, 100)

	for mapTask := 0; mapTask < nMap; mapTask++ {
		inFile := reduceName(jobName, mapTask, reduceTask)
		f, err := os.Open(inFile)
		if err != nil {
			continue
		}

		r := csv.NewReader(f)
		for {
			record, err := r.Read()
			if err != nil {
				break
			}
			records = append(records, KeyValue{record[0], record[1]})
		}
	}

	sort.Sort(sortRecordByKey(records))

	enc := json.NewEncoder(out)

	var (
		k     string
		start int
	)
	if len(records) > 0 {
		k = records[0].Key
	}

	for i, v := range records {
		// a new Key
		if v.Key != k {
			values := make([]string, 0, i-start+1)
			for _, record := range records[start:i] {
				values = append(values, record.Value)
			}
			enc.Encode(KeyValue{k, reduceF(k, values)})

			start = i
			k = v.Key
		}
	}

	if k != "" && start < len(records) {
		values := make([]string, 0, len(records)-start+1)
		for _, record := range records[start:] {
			values = append(values, record.Value)
		}
		enc.Encode(KeyValue{k, reduceF(k, values)})
	}
}

type sortRecordByKey []KeyValue

func (r sortRecordByKey) Len() int { return len(r) }

func (r sortRecordByKey) Swap(i, j int) { r[i], r[j] = r[j], r[i] }

func (r sortRecordByKey) Less(i, j int) bool { return r[i].Key < r[j].Key }
