package executor

import (
	"bytes"
	"flag"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/naturali/kmr/bucket"
	"github.com/naturali/kmr/master"
	kmrpb "github.com/naturali/kmr/pb"
	"github.com/naturali/kmr/records"
	"github.com/naturali/kmr/util"
	"github.com/naturali/kmr/util/log"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	FLUSH_SIZE = 80 * 1024 * 1024 // 80M
)

var (
	jobName    = flag.String("jobname", "wc", "jobName")
	inputFile  = flag.String("file", "", "input file path")
	dataDir    = flag.String("intermediate-dir", "/tmp/", "directory of intermediate files")
	phase      = flag.String("phase", "", "map or reduce")
	nMap       = flag.Int("nMap", 1, "number of mappers")
	nReduce    = flag.Int("nReduce", 1, "number of reducers")
	mapID      = flag.Int("mapID", 0, "mapper id")
	reduceID   = flag.Int("reduceID", 0, "reducer id")
	readerType = flag.String("reader-type", "textfile", "type of record reader for input files")

	masterAddr = flag.String("master-addr", "", "the address of master")
)

type ComputeWrap struct {
	mapFunc    func(kvs <-chan *kmrpb.KV) <-chan *kmrpb.KV
	reduceFunc func(key []byte, values [][]byte) []*kmrpb.KV
}

func (cw *ComputeWrap) BindMapper(mapper func(kvs <-chan *kmrpb.KV) <-chan *kmrpb.KV) {
	cw.mapFunc = mapper
}

func (cw *ComputeWrap) BindReducer(reducer func(key []byte, values [][]byte) []*kmrpb.KV) {
	cw.reduceFunc = reducer
}

func (cw *ComputeWrap) Run() {
	flag.Parse()

	if os.Getenv("KMR_MASTER_ADDRESS") != "" {
		addr := os.Getenv("KMR_MASTER_ADDRESS")
		masterAddr = &addr
	}

	if *masterAddr == "" {
		// Local Run
		var taskID int
		switch *phase {
		case "map":
			taskID = *mapID
		case "reduce":
			taskID = *reduceID
		}
		err := cw.phaseSelector(*jobName, *phase, *dataDir, *inputFile, *nMap, *nReduce, *readerType, taskID, 0, make([]int64, *nMap))
		if err != nil {
			log.Fatal(err)
		}
	} else {
		var retcode kmrpb.ReportInfo_ErrorCode
		// Distributed Mode
		cc, err := grpc.Dial(*masterAddr, grpc.WithInsecure())
		if err != nil {
			log.Fatal("cannot connect to master", err)
		}
		masterClient := kmrpb.NewMasterClient(cc)
		for {
			task, err := masterClient.RequestTask(context.Background(), &kmrpb.RegisterParams{
				JobName: *jobName,
			})
			if err != nil || task.Retcode != 0 {
				log.Error(err)
				// TODO: random backoff
				time.Sleep(1 * time.Second)
				continue
			}
			taskInfo := task.Taskinfo
			timer := time.NewTicker(master.HEARTBEAT_TIMEOUT / 2)
			go func() {
				for range timer.C {
					// SendHeartBeat
					masterClient.ReportTask(context.Background(), &kmrpb.ReportInfo{
						JobName:  *jobName,
						Phase:    taskInfo.Phase,
						TaskID:   taskInfo.TaskID,
						WorkerID: task.WorkerID,
						Retcode:  kmrpb.ReportInfo_DOING,
					})
				}
			}()
			err = cw.phaseSelector(taskInfo.JobName, taskInfo.Phase, taskInfo.IntermediateDir, taskInfo.File,
				int(taskInfo.NMap), int(taskInfo.NReduce), taskInfo.ReaderType, int(taskInfo.TaskID), task.WorkerID, taskInfo.CommitMappers)
			retcode = kmrpb.ReportInfo_FINISH
			if err != nil {
				log.Debug(err)
				retcode = kmrpb.ReportInfo_ERROR
			}
			timer.Stop()
			masterClient.ReportTask(context.Background(), &kmrpb.ReportInfo{
				JobName:  *jobName,
				Phase:    taskInfo.Phase,
				TaskID:   taskInfo.TaskID,
				WorkerID: task.WorkerID,
				Retcode:  retcode,
			})
			// backoff
			if err != nil {
				time.Sleep(1 * time.Second)
			}
		}
	}
}

func (cw *ComputeWrap) phaseSelector(jobName string, phase string, intermediateDir string, file string,
	nMap int, nReduce int, readerType string, taskID int, workerID int64,
	commitMappers []int64) error {
	bk, err := bucket.NewFilePool(intermediateDir + "/" + jobName)
	if err != nil {
		log.Fatalf("Fail to open bucket: %v", err)
	}
	switch phase {
	case "map":
		log.Infof("starting id%d mapper", taskID)

		rr := records.MakeRecordReader(readerType, map[string]interface{}{"filename": file})
		// Mapper
		if err := cw.doMap(rr, bk, taskID, nReduce, workerID); err != nil {
			log.Fatalf("Fail to Map: %v", err)
		}
	case "reduce":
		log.Infof("starting id%d reducer", nReduce)

		// Reduce
		outputFile := "res-" + strconv.Itoa(taskID) + ".t"
		writer, err := bk.OpenWrite(outputFile)
		if err != nil {
			log.Fatalf("Failed to open intermediate: %v", err)
		}
		if err = cw.doReduce(bk, taskID, nMap, commitMappers, writer); err != nil {
			log.Fatalf("Fail to Reduce: %v", err)
		}
		writer.Close()
	default:
		panic("bad phase")
	}
	log.Info("Exit executor")
	return nil
}

// doMap does map operation and save the intermediate files.
func (cw *ComputeWrap) doMap(rr records.RecordReader, bk bucket.Bucket, mapID int, nReduce int, workerID int64) (err error) {
	startTime := time.Now()
	aggregated := make([]*records.Record, 0)
	flushOutFiles := make([]string, 0)
	currentAggregatedSize := 0

	// map
	waitc := make(chan struct{})
	inputKV := make(chan *kmrpb.KV, 1024)
	outputKV := cw.mapFunc(inputKV)
	go func() {
		var waitFlushWrite sync.WaitGroup
		for in := range outputKV {
			aggregated = append(aggregated, KVToRecord(in))
			currentAggregatedSize += 8 + len(in.Key) + len(in.Value)
			if currentAggregatedSize >= FLUSH_SIZE {

				filename := bucket.FlushoutFileName("map", mapID, len(flushOutFiles), workerID)
				waitFlushWrite.Add(1)
				go func(filename string, data []*records.Record) {
					writer, err := bk.OpenWrite(filename)
					if err != nil {
						log.Fatal(err)
					}
					sort.Slice(data, func(i, j int) bool {
						return bytes.Compare(data[i].Key, data[j].Key) < 0
					})
					for _, r := range data {
						if err := writer.WriteRecord(r); err != nil {
							log.Fatal(err)
						}
					}
					if err := writer.Close(); err != nil {
						log.Fatal(err)
					}
					waitFlushWrite.Done()
				}(filename, aggregated)

				aggregated = make([]*records.Record, 0)
				currentAggregatedSize = 0
				flushOutFiles = append(flushOutFiles, filename)
			}
		}
		sort.Slice(aggregated, func(i, j int) bool {
			return bytes.Compare(aggregated[i].Key, aggregated[j].Key) < 0
		})
		waitFlushWrite.Wait()
		close(waitc)
	}()
	for rr.HasNext() {
		inputKV <- RecordToKV(rr.Pop())
	}
	close(inputKV)
	<-waitc
	log.Debug("DONE Map. Took:", time.Since(startTime))

	readers := make([]records.RecordReader, 0)
	for _, file := range flushOutFiles {
		reader, err := bk.OpenRead(file)
		if err != nil {
			log.Fatalf("Failed to open intermediate: %v", err)
		}
		readers = append(readers, reader)
	}
	readers = append(readers, records.MakeRecordReader("memory", map[string]interface{}{"data": aggregated}))
	sorted := make(chan *records.Record, 1024)
	go records.MergeSort(readers, sorted)

	writers := make([]records.RecordWriter, 0)
	for i := 0; i < nReduce; i++ {
		intermediateFileName := bucket.IntermediateFileName(mapID, i, workerID)
		writer, err := bk.OpenWrite(intermediateFileName)
		if err != nil {
			log.Fatalf("Failed to open intermediate: %v", err)
		}
		writers = append(writers, writer)
	}
	for r := range sorted {
		rBucketID := util.HashBytesKey(r.Key) % nReduce
		writers[rBucketID].WriteRecord(r)
	}
	for i := 0; i < nReduce; i++ {
		writers[i].Close()
	}

	log.Debug("FINISH Write IntermediateFiles. Took:", time.Since(startTime))
	return
}

// doReduce does reduce operation
func (cw *ComputeWrap) doReduce(bk bucket.Bucket, reduceID int, nMap int, commitMappers []int64, writer records.RecordWriter) error {
	startTime := time.Now()
	readers := make([]records.RecordReader, 0)
	for i := 0; i < nMap; i++ {
		reader, err := bk.OpenRead(bucket.IntermediateFileName(i, reduceID, commitMappers[i]))
		if err != nil {
			log.Fatalf("Failed to open intermediate: %v", err)
		}
		readers = append(readers, reader)
	}
	sorted := make(chan *records.Record, 1024)
	go records.MergeSort(readers, sorted)
	var lastKey []byte
	values := make([][]byte, 0)
	for r := range sorted {
		if bytes.Equal(lastKey, r.Key) {
			values = append(values, r.Value)
			continue
		}
		if lastKey != nil {
			res, _ := cw.doReduceForSingleKey(lastKey, values)
			for _, r := range res {
				writer.WriteRecord(KVToRecord(r))
			}
		}
		values = values[:0]
		lastKey = r.Key
		values = append(values, r.Value)
	}
	if lastKey != nil {
		res, _ := cw.doReduceForSingleKey(lastKey, values)
		for _, r := range res {
			writer.WriteRecord(KVToRecord(r))
		}
	}
	log.Debug("DONE Reduce. Took:", time.Since(startTime))
	return nil
}

func (cw *ComputeWrap) doReduceForSingleKey(key []byte, values [][]byte) ([]*kmrpb.KV, error) {
	return cw.reduceFunc(key, values), nil
}

// RecordToKV converts an Record to a kmrpb.KV
func RecordToKV(record *records.Record) *kmrpb.KV {
	return &kmrpb.KV{Key: record.Key, Value: record.Value}
}

// KVToRecord converts a kmrpb.KV to an Record
func KVToRecord(kv *kmrpb.KV) *records.Record {
	return &records.Record{Key: kv.Key, Value: kv.Value}
}
