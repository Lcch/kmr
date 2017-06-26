package main

import (
	"regexp"
	"strings"
	"unicode"

	"github.com/naturali/kmr/executor"
	kmrpb "github.com/naturali/kmr/pb"
)

const (
	WasherSplitPuncts  = `,|\.|!|\?|，|。|！|？|:|：|;|；|「|」|．|\t|：…｛｝`
	WasherIgnorePuncts = " 　'\"《》‘’“”・-_<>〃〈〉()（）……@、【】[]*-、『』~"
)

var (
	ignorePuncts map[rune]bool
	splitPuncts  map[rune]bool
)

func isAlphaOrNumber(r rune) bool {
	return 'a' <= r && r <= 'z' || 'A' <= r && r <= 'Z' || unicode.IsDigit(r)
}

func isChinese(r rune) bool {
	return r >= '\u4e00' && r <= '\u9fa5'
}

func isIgnorePuncts(r rune) bool {
	_, ok := ignorePuncts[r]
	return ok
}

func isSplitPuncts(r rune) bool {
	if _, ok := splitPuncts[r]; ok {
		return true
	}
	return false
}

func RemoveIllegalPattern(line string) string {
	if strings.HasPrefix(line, "<docno>") || strings.HasSuffix(line, "<url>") {
		return ""
	}
	line = strings.Replace(strings.Replace(line, "</a>", "", -1), "<a>", "", -1)
	re, _ := regexp.Compile(`^https?://.*[\r\n]*`)
	line = re.ReplaceAllString(line, "")
	return line
}

func ProcessSingleSentence(line string) []string {
	outputs := make([]string, 0)

	out := make([]string, 0)
	e_word := ""
	for _, r := range line {
		if isSplitPuncts(r) {
			if len(e_word) > 0 {
				out = append(out, e_word)
				e_word = ""
			}
			if len(out) > 0 {
				outputs = append(outputs, strings.Join(out, " "))
			}
			out = out[:0]
		} else if isIgnorePuncts(r) {
			if len(e_word) > 0 {
				out = append(out, e_word)
				e_word = ""
			}
		} else if isChinese(r) {
			if len(e_word) > 0 {
				out = append(out, e_word)
				e_word = ""
			}
			out = append(out, string(r))
		} else if isAlphaOrNumber(r) {
			e_word += string(r)
		} else {
			return nil
		}
	}
	if len(e_word) > 0 {
		out = append(out, e_word)
	}
	if len(out) > 0 {
		outputs = append(outputs, strings.Join(out, " "))
	}
	return outputs
}

func Map(kvs <-chan *kmrpb.KV) <-chan *kmrpb.KV {
	out := make(chan *kmrpb.KV, 1024)
	go func() {
		for kv := range kvs {
			sentence := RemoveIllegalPattern(strings.Trim(string(kv.Value), "\n"))
			for _, procceed := range ProcessSingleSentence(sentence) {
				out <- &kmrpb.KV{Key: []byte(procceed), Value: []byte{1}}
			}
		}
		close(out)
	}()
	return out
}

func Reduce(key []byte, values [][]byte) []*kmrpb.KV {
	out := make([]*kmrpb.KV, 0)
	out = append(out, &kmrpb.KV{Key: key, Value: []byte{1}})
	return out
}

func main() {
	ignorePuncts = make(map[rune]bool)
	splitPuncts = make(map[rune]bool)
	for _, r := range WasherIgnorePuncts {
		ignorePuncts[r] = true
	}
	for _, r := range WasherSplitPuncts {
		splitPuncts[r] = true
	}

	cw := &executor.ComputeWrap{}
	cw.BindMapper(Map)
	cw.BindReducer(Reduce)
	cw.Run()
}
