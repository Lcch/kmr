package main

import (
	"encoding/json"

	"github.com/naturali/kmr/cli"
	"github.com/naturali/kmr/jobgraph"
	"github.com/naturali/kmr/mapred"
	"github.com/naturali/kmr/proto"
	"github.com/naturali/kmr/util/log"

	"fmt"
	"github.com/golang/protobuf/proto"
)

type noIdeaMap struct {
	mapred.MapperCommon
}

type noIdeaReduce struct {
	mapred.ReducerCommon
}

func sliceUniqMap(s []string) []string {
	seen := make(map[string]struct{}, len(s))
	j := 0
	for _, v := range s {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		s[j] = v
		j++
	}
	return s[:j]
}

func fetchAllTexts(page map[string]interface{}) []string {
	ret := make([]string, 0)
	if nodes, ok := page["nodes"].([]interface{}); ok {
		for _, n := range nodes {
			if sn, ok := n.(map[string]interface{}); ok {
				ret = append(ret, fetchAllTexts(sn)...)
			}
		}
	}
	if c, ok := page["content-desc"].(string); ok && len(c) > 0 {
		ret = append(ret, c)
	}
	if c, ok := page["text"].(string); ok && len(c) > 0 {
		ret = append(ret, c)
	}
	return ret
}

func (w *noIdeaMap) Map(key interface{}, value interface{}, output func(k, v interface{}), reporter interface{}) {
	sessions := nilog.Sessions{}
	if err := proto.Unmarshal(value.([]byte), &sessions); err != nil {
		log.Error(err)
	} else {
		for _, sess := range sessions.Content {
			for _, e := range sess.Logs.Content {
				if e.Action == "query" && e.QueryID != "" {
					kvs := map[string]interface{}{}
					err := json.Unmarshal(e.KVs, &kvs)
					if err != nil {
						log.Error(err, "e.KVs")
						continue
					}
					if state, ok := kvs["success"].(string); ok && state == "success" {
						planName, _ := kvs["plan_name"].(string)
						pkg, _ := kvs["open_package_name"].(string)
						pkgVersionInFloat, _ := kvs["open_package_version"].(float64)
						pkgVersion := int64(pkgVersionInFloat)
						if pkg == "com.singulariti.niapp" || len(pkg) == 0 || len(planName) == 0 || planName == "OpenApp" {
							continue
						}
						if pds, ok := kvs["page_desc"].([]interface{}); ok {
							for _, pageInterface := range pds {
								page := map[string]interface{}{}
								if pageStr, ok := pageInterface.(string); ok {
									err = json.Unmarshal([]byte(pageStr), &page)
									if err != nil {
										continue
									}
									pageTopActivity, _ := page["top-activity"].(string)
									pagePkg, _ := page["pkg"].(string)
									if pkg == pagePkg {
										log.Debugf("%s %s", pageTopActivity, pagePkg)
										for _, text := range sliceUniqMap(fetchAllTexts(page)) {
											//log.Debugf("%s %s %d %s %s", planName, pkg, pkgVersion, pageTopActivity, text)
											output(fmt.Sprintf("%s %s %d %s %s", planName, pkg, pkgVersion, pageTopActivity, text), uint32(1))
										}
									}
								}
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
	job.SetName("post-page")

	files := make([]string, 0)
	for date := 27; date <= 27; date++ {
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
	job.AddJobNode(inputs, "PostPage").
		AddMapper(mapper, 1).
		AddReducer(reducer, 3).
		SetCombiner(reducer)
	cli.Run(&job)
}
