package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/naturali/kmr/bucket"
	"github.com/naturali/kmr/jobgraph"
	"github.com/naturali/kmr/master"
	kmrpb "github.com/naturali/kmr/pb"
	"github.com/naturali/kmr/records"
	"github.com/naturali/kmr/util/log"

	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/util/rand"
)

// XXX this should be unified with master.map/reducePhase
const (
	mapPhase    = "map"
	reducePhase = "reduce"
)

type Worker struct {
	job                                  *jobgraph.Job
	mapBucket, interBucket, reduceBucket bucket.Bucket
	workerID                             int64
	flushOutSize                         int
	masterAddr                           string
}

// NewWorker create a worker
func NewWorker(job *jobgraph.Job, workerID int64, masterAddr string, flushOutSize int, mapBucket, interBucket, reduceBucket bucket.Bucket) *Worker {
	worker := Worker{
		job:          job,
		masterAddr:   masterAddr,
		mapBucket:    mapBucket,
		interBucket:  interBucket,
		reduceBucket: reduceBucket,
		workerID:     workerID,
		flushOutSize: flushOutSize,
	}
	return &worker
}

func (w *Worker) getBucket(files jobgraph.Files) bucket.Bucket {
	switch files.GetBucketType() {
	case jobgraph.MapBucket:
		return w.mapBucket
	case jobgraph.ReduceBucket:
		return w.reduceBucket
	case jobgraph.InterBucket:
		return w.interBucket
	}
	return nil
}

func (w *Worker) Run() {
	//Here we must know the master address
	var retcode kmrpb.ReportInfo_ErrorCode

	cc, err := grpc.Dial(w.masterAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatal("cannot connect to master", err)
	}
	masterClient := kmrpb.NewMasterClient(cc)
	for {
		task, err := masterClient.RequestTask(context.Background(), &kmrpb.RegisterParams{
			JobName:  w.job.GetName(),
			WorkerID: w.workerID,
		})
		if err != nil || task.Retcode != 0 {
			log.Error(err)
			// TODO: random backoff
			time.Sleep(1 * time.Second)
			continue
		}
		taskInfo := task.Taskinfo
		timer := time.NewTicker(master.HeartBeatTimeout / 2)
		go func() {
			for range timer.C {
				// SendHeartBeat
				masterClient.ReportTask(context.Background(), &kmrpb.ReportInfo{
					TaskInfo: taskInfo,
					WorkerID: w.workerID,
					Retcode:  kmrpb.ReportInfo_DOING,
				})
			}
		}()

		w.executeTask(taskInfo)

		retcode = kmrpb.ReportInfo_FINISH
		if err != nil {
			log.Debug(err)
			retcode = kmrpb.ReportInfo_ERROR
		}
		timer.Stop()
		masterClient.ReportTask(context.Background(), &kmrpb.ReportInfo{
			TaskInfo: taskInfo,
			WorkerID: w.workerID,
			Retcode:  retcode,
		})
		// backoff
		if err != nil {
			time.Sleep(time.Duration(rand.IntnRange(1, 3)) * time.Second)
		}
	}
}

func (w *Worker) executeTask(task *kmrpb.TaskInfo) (err error) {
	cw := &ComputeWrapClass{}
	err = nil
	mapredNode := w.job.GetMapReduceNode(task.JobNodeName, int(task.MapredNodeIndex))
	if mapredNode == nil {
		x, _ := json.Marshal(task)
		err = errors.New(fmt.Sprint("Cannot find mapred node", x))
		log.Error(err)
		return
	}
	cw.BindMapper(mapredNode.GetMapper())
	cw.BindReducer(mapredNode.GetReducer())
	cw.BindCombiner(mapredNode.GetCombiner())
	switch task.Phase {
	case mapPhase:
		w.runMapper(cw, mapredNode, task.SubIndex)
	case reducePhase:
		w.runReducer(cw, mapredNode, task.SubIndex)
	default:
		x, _ := json.Marshal(task)
		err = errors.New(fmt.Sprint("Unkown task phase", x))
	}
	return err
}

func (w *Worker) runReducer(cw *ComputeWrapClass, node *jobgraph.MapReduceNode, subIndex int32) {
	readers := make([]records.RecordReader, 0)
	interFiles := node.GetInterFileNameGenerator().GetReducerInputFiles(int(subIndex))
	for _, interFile := range interFiles {
		//TODO: How to deal with backup Task ?
		reader, err := w.interBucket.OpenRead(interFile)
		recordReader := records.NewStreamRecordReader(reader)
		if err != nil {
			log.Fatalf("Failed to open intermediate: %v", err)
		}
		readers = append(readers, recordReader)
	}

	outputFile := node.GetOutputFiles().GetFiles()[subIndex]
	writer, err := w.getBucket(node.GetOutputFiles()).OpenWrite(outputFile)
	if err != nil {
		log.Fatalf("Failed to open intermediate: %v", err)
	}
	recordWriter := records.MakeRecordWriter("stream", map[string]interface{}{"writer": writer})
	if err := cw.DoReduce(readers, recordWriter); err != nil {
		log.Fatalf("Fail to Reduce: %v", err)
	}
	recordWriter.Close()
}

func (w *Worker) runMapper(cw *ComputeWrapClass, node *jobgraph.MapReduceNode, subIndex int32) {
	startTime := time.Now()

	// Inputs Files
	inputFiles := node.GetInputFiles().GetFiles()
	readers := make([]records.RecordReader, 0)
	for fidx := int(subIndex) * node.GetMapperBatchSize(); fidx < len(inputFiles) && fidx < int(subIndex+1)*node.GetMapperBatchSize(); fidx++ {
		file := inputFiles[fidx]
		log.Debug("Opening mapper input file", file)
		reader, err := w.getBucket(node.GetInputFiles()).OpenRead(file)
		if err != nil {
			log.Fatalf("Fail to open object %s: %v", file, err)
		}
		recordReader := records.MakeRecordReader(node.GetInputFiles().GetType(), map[string]interface{}{"reader": reader})
		readers = append(readers, recordReader)
	}
	batchReader := records.NewChainReader(readers)

	// Intermediate writers
	interFiles := node.GetInterFileNameGenerator().GetMapperOutputFiles(int(subIndex))
	if len(interFiles) != node.GetReducerNum() {
		//XXX: this should be done in validateGraph
		log.Fatal("mapper output files count doesn't equal to reducer count")
	}

	writers := make([]records.RecordWriter, 0)
	for i := 0; i < node.GetReducerNum(); i++ {
		intermediateFileName := interFiles[i]
		writer, err := w.interBucket.OpenWrite(intermediateFileName)
		recordWriter := records.MakeRecordWriter("stream", map[string]interface{}{"writer": writer})
		if err != nil {
			log.Fatalf("Failed to open intermediate: %v", err)
		}
		writers = append(writers, recordWriter)
	}

	cw.DoMap(batchReader, writers, w.interBucket, w.flushOutSize, node.GetIndex(), node.GetReducerNum(), w.workerID)

	//master should delete intermediate files
	for _, reader := range readers {
		reader.Close()
	}
	for _, writer := range writers {
		writer.Close()
	}

	log.Debug("FINISH runMapper. Took:", time.Since(startTime))
}
