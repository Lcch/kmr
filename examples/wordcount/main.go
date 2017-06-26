package main

import (
	"strconv"
	"strings"
	"unicode"

	"github.com/naturali/kmr/executor"
	kmrpb "github.com/naturali/kmr/pb"
)

func Map(kvs <-chan *kmrpb.KV) <-chan *kmrpb.KV {
	out := make(chan *kmrpb.KV, 1024)
	go func() {
		for kv := range kvs {
			for _, key := range strings.FieldsFunc(string(kv.Value), func(c rune) bool {
				return !unicode.IsLetter(c)
			}) {
				out <- &kmrpb.KV{Key: []byte(key), Value: []byte(strconv.Itoa(1))}
			}
		}
		close(out)
	}()
	return out
}

func Reduce(key []byte, values [][]byte) []*kmrpb.KV {
	out := make([]*kmrpb.KV, 0)
	out = append(out, &kmrpb.KV{Key: key, Value: []byte(strconv.Itoa(len(values)))})
	return out
}

func main() {
	cw := &executor.ComputeWrap{}
	cw.BindMapper(Map)
	cw.BindReducer(Reduce)
	cw.Run()
}
