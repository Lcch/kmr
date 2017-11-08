package main

import (
	"encoding/binary"
	"regexp"
	"strings"
	"unicode"

	"github.com/naturali/kmr/cli"
	"github.com/naturali/kmr/jobgraph"
	"github.com/naturali/kmr/mapred"
)

const (
	WasherSplitPuncts  = `,|\.|!|\?|，|。|！|？|:|：|;|；|「|」|．|\t|：…｛｝`
	WasherIgnorePuncts = " 　'\"《》‘’“”・-_<>〃〈〉()（）……@、【】[]*-、『』~"
)

// removeIllegalPattern removes doc id and http tags in the line.
func removeIllegalPattern(line string) string {
	if strings.HasPrefix(line, "<docno>") || strings.HasSuffix(line, "<url>") {
		return ""
	}
	line = strings.Replace(strings.Replace(line, "</a>", "", -1), "<a>", "", -1)
	re, _ := regexp.Compile(`^https?://.*[\r\n]*`)
	line = re.ReplaceAllString(line, "")
	return line
}

// isAlphaOrNumber determines whether a rune is a digit or english character.
func isAlphaOrNumber(r rune) bool {
	return 'a' <= r && r <= 'z' || 'A' <= r && r <= 'Z' || unicode.IsDigit(r)
}

// isChinese determines whether a rune is a Chinese.
func isChinese(r rune) bool {
	return r >= '\u4e00' && r <= '\u9fa5'
}

// tokenizeWords splits the line into multiple words. It only keeps Chinese characters, English word and numbers.
// It ignores all of invalid characters.
// For example:
// 		tokenizeWords("我的iphone7 is mine!!!!!") = []string{"我", "的", "iphone7", "is", "mine"}
func tokenizeWords(line string) []string {
	outputs := make([]string, 0)
	englishWord := ""
	for _, r := range line {
		if isChinese(r) {
			if len(englishWord) > 0 {
				outputs = append(outputs, englishWord)
				englishWord = ""
			}
			outputs = append(outputs, string(r))
		} else if isAlphaOrNumber(r) {
			englishWord += string(r)
		} else {
			if len(englishWord) > 0 {
				outputs = append(outputs, englishWord)
				englishWord = ""
			}
		}
	}
	if len(englishWord) > 0 {
		outputs = append(outputs, englishWord)
	}
	return outputs
}

type wordCountMap struct {
	mapred.MapperCommon
}

type wordCountReduce struct {
	mapred.ReducerCommon
}

// Map Value is lines from file. Map function split lines into words and emit (word, 1) pairs
func (*wordCountMap) Map(key interface{}, value interface{},
	output func(k, v interface{}), reporter interface{}) {
	var maxWordLength = 20
	v, _ := value.(string)
	for _, procceed := range tokenizeWords(strings.Trim(v, "\n")) {
		if len(procceed) > maxWordLength {
			continue
		}
		output(procceed, uint64(1))
	}
}

// Reduce key is word and valueNext is an iterator function. Add all values of one key togather to count the word occurs
func (*wordCountReduce) Reduce(key interface{}, valuesNext mapred.ValueIterator,
	output func(v interface{}), reporter interface{}) {
	var count uint64
	mapred.ForEachValue(valuesNext, func(value interface{}) {
		val, _ := value.(uint64)
		count += val
	})
	output(count)
}

func wordCountCombiner(v1 []byte, v2 []byte) []byte {
	var count uint64
	count += binary.LittleEndian.Uint64(v1)
	count += binary.LittleEndian.Uint64(v2)
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, count)
	return b
}

// It defines the map-reduce of word-count which is counting the number of each word show-ups in the corpus.
func NewWordCountMapReduce() (*wordCountMap, *wordCountReduce, func(v1 []byte, v2 []byte) []byte) {
	wcmap := &wordCountMap{
		MapperCommon: mapred.MapperCommon{
			TypeConverters: mapred.TypeConverters{
				InputKeyTypeConverter:    mapred.Bytes{},
				InputValueTypeConverter:  mapred.String{},
				OutputKeyTypeConverter:   mapred.String{},
				OutputValueTypeConverter: mapred.Uint64{},
			},
		},
	}
	wcreduce := &wordCountReduce{
		ReducerCommon: mapred.ReducerCommon{
			TypeConverters: mapred.TypeConverters{
				InputKeyTypeConverter:    mapred.String{},
				InputValueTypeConverter:  mapred.Uint64{},
				OutputKeyTypeConverter:   mapred.String{},
				OutputValueTypeConverter: mapred.Uint64{},
			},
		},
	}
	return wcmap, wcreduce, wordCountCombiner
}

func main() {
	mapper, reducer, combiner := NewWordCountMapReduce()

	var job jobgraph.Job
	job.SetName("wordcount")

	input := &jobgraph.InputFiles{
		Files: []string{
			"/octp/sogout/sogout/outputs/filtered/sogout_data.1/part-m-0000.bz2",
		},
		Type: "bz2",
	}
	job.AddJobNode(input, "wordcount").
		AddMapper(mapper, 1).
		AddReducer(reducer, 3).
		SetCombiner(combiner)
	cli.Run(&job)
}
