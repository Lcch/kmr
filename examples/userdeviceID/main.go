package main

import (
	"github.com/naturali/kmr/cli"
	"github.com/naturali/kmr/jobgraph"
	"github.com/naturali/kmr/mapred"
	"github.com/naturali/kmr/proto"
	"github.com/naturali/kmr/util/log"

	"fmt"
	"github.com/golang/protobuf/proto"
	"strconv"
	"time"
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
					date := time.Unix(e.Timestamp/1000, e.Timestamp%1000)
					output(date.Month().String()+strconv.Itoa(date.Day())+"_"+e.DeviceID, uint32(1))
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
	job.SetName("userdevice")

	files := make([]string, 0)
	for date := 24; date <= 31; date++ {
		for iter := 0; iter < 1024; iter++ {
			files = append(files, fmt.Sprintf("shard-%d/2017-08-%d", iter, date))
		}
	}
	fmt.Println(files)
	inputs := &jobgraph.InputFiles{
		// put a.t in the map bucket directory
		Files: files,
		Type:  "readAllBytes",
	}
	job.AddJobNode(inputs, "userdeviceid").
		AddMapper(mapper, 1).
		AddReducer(reducer, 1)
	cli.Run(&job)
}
