package main

import (
	"encoding/json"
	"fmt"

	"github.com/naturali/kmr/cli"
	"github.com/naturali/kmr/jobgraph"
	"github.com/naturali/kmr/mapred"
	"github.com/naturali/kmr/proto"
	"github.com/naturali/kmr/util/log"

	"github.com/golang/protobuf/proto"
)

type noIdeaMap struct {
	mapred.MapperCommon
}

type noIdeaReduce struct {
	mapred.ReducerCommon
}

type tupleClass struct {
	deviceID    string `json:"d"`
	topActivity string `json:"ta"`
	Text        string `json:"t"`
}

func (w *noIdeaMap) Map(key interface{}, value interface{}, output func(k, v interface{}), reporter interface{}) {
	sessions := nilog.Sessions{}
	if err := proto.Unmarshal(value.([]byte), &sessions); err != nil {
		log.Error(err)
	} else {
		for _, sess := range sessions.Content {
			for _, e := range sess.Logs.Content {
				if e.Action == "query" && e.DeviceID != "" {
					if e.Timestamp >= 1504598400000 {
						//date := time.Unix(e.Timestamp/1000, e.Timestamp%1000)
						//log.Debug(e.Timestamp, " ", date, e.QueryID)
						kvs := map[string]interface{}{}
						err := json.Unmarshal(e.KVs, &kvs)
						if err != nil {
							log.Error(err, "e.KVs")
							continue
						}

						src := kvs["result_src"].(string)
						if src == "" {
							continue
						}
						abt, ok := kvs["abtesting"].(map[string]interface{})
						if !ok {
							continue
						}
						if v, ok := abt["zir-blank-interval"].(string); ok {
							output(v+"=tot", uint32(1))
							output(v+"_"+e.DeviceID, uint32(1))
							if src != "zir" {
								if e.Query != "" {
									output(v+"#"+src+"_ne", uint32(1))
								} else {
									output(v+"#"+src+"_e", uint32(1))
								}
							} else {
								output(v+"#"+src, uint32(1))
							}
						}
					}
				}
			}
		}
	}
}

func (*noIdeaReduce) Reduce(key interface{}, valuesNext mapred.ValueIterator, output func(v interface{}), reporter interface{}) {
	var count uint32
	mapred.ForEachValue(valuesNext, func(value interface{}) {
		val, _ := value.(uint32)
		count += val
	})
	output(count)
}

func main() {
	mapper := &noIdeaMap{
		MapperCommon: mapred.MapperCommon{
			TypeConverters: mapred.TypeConverters{
				InputKeyTypeConverter:    mapred.Bytes{},
				InputValueTypeConverter:  mapred.Bytes{},
				OutputKeyTypeConverter:   mapred.String{},
				OutputValueTypeConverter: mapred.Uint32{},
			},
		},
	}
	reducer := &noIdeaReduce{
		ReducerCommon: mapred.ReducerCommon{
			TypeConverters: mapred.TypeConverters{
				InputKeyTypeConverter:    mapred.String{},
				InputValueTypeConverter:  mapred.Uint32{},
				OutputKeyTypeConverter:   mapred.String{},
				OutputValueTypeConverter: mapred.Uint32{},
			},
		},
	}

	var job jobgraph.Job
	job.SetName("zir")

	files := make([]string, 0)

	for iter := 0; iter < 10; iter++ {
		files = append(files, fmt.Sprintf("shard-%d/2017-09-05", iter))
	}
	fmt.Println(files)
	inputs := &jobgraph.InputFiles{
		// put a.t in the map bucket directory
		Files: files,
		Type:  "readAllBytes",
	}
	job.AddJobNode(inputs, "zir").
		AddMapper(mapper, 1).
		AddReducer(reducer, 1).SetCombiner(reducer)
	cli.Run(&job)
}
