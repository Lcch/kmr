package jobgraph

import (
	"unicode"

	"github.com/naturali/kmr/mapred"
	"github.com/naturali/kmr/util/log"
	"github.com/naturali/kmr/util"
)

type MapReduceNode struct {
	index   int
	mapper  mapred.Mapper
	reducer mapred.Reducer
	jobNode *JobNode

	mapperBatchSize         int
	reducerCount            int
	interFiles              InterFileNameGenerator
	inputFiles, outputFiles Files

	chainPrev, chainNext *MapReduceNode
}

type JobNode struct {
	name                       string
	startNode, endNode         *MapReduceNode
	dependencies, dependencyOf []*JobNode
	graph                      *Job
}

type Job struct {
	roots       []*JobNode
	mrNodes     []*MapReduceNode
	mrNodeIndex int
	name        string
}

func (node *MapReduceNode) isEndNode() (res bool) {
	res = node == node.jobNode.endNode
	return
}

func (node *MapReduceNode) GetInputFiles() Files {
	return node.inputFiles
}

func (node *MapReduceNode) GetOutputFiles() Files {
	return node.outputFiles
}

func (node *MapReduceNode) GetInterFileNameGenerator() *InterFileNameGenerator {
	res := node.interFiles
	// prevent from writing
	return &res
}

func (node *MapReduceNode) GetMapperNum() int {
	if node.inputFiles == nil || node.mapperBatchSize == 0{
		return 0
	}
	return (len(node.inputFiles.GetFiles()) + node.mapperBatchSize - 1) / node.mapperBatchSize
}

func (node *MapReduceNode) GetReducerNum() int {
	return node.reducerCount
}

func (node *MapReduceNode) GetMapper() mapred.Mapper {
	return node.mapper
}

func (node *MapReduceNode) GetReducer() mapred.Reducer {
	return node.reducer
}

func (node *MapReduceNode) ToJobDesc() *JobDescription {
	return &JobDescription{
		JobNodeName:      node.jobNode.name,
		MapReduceNodeIndex:  int32(node.index),
		MapperObjectSize: len(node.inputFiles.GetFiles()),
		MapperBatchSize:  node.mapperBatchSize,
		ReducerNumber:    node.reducerCount,
	}
}


func (n *JobNode) AddMapper(mapper mapred.Mapper, inputs Files, batchSize ...int) *JobNode {
	if len(batchSize) == 0 {
		batchSize = append(batchSize, 1)
	}
	if n.endNode.reducer != nil {
		mrnode := &MapReduceNode{
			index:           n.graph.mrNodeIndex,
			jobNode:         n,
			mapper:          mapper,
			chainPrev:       n.endNode,
			inputFiles:      n.endNode.outputFiles,
			mapperBatchSize: batchSize[0],
		}
		mrnode.interFiles.mrNode = mrnode
		n.graph.mrNodeIndex++
		n.endNode.chainNext = mrnode
		n.endNode = mrnode
		n.graph.mrNodes = append(n.graph.mrNodes, mrnode)
	} else {
		//use origin
		n.endNode.mapper = combineMappers(n.endNode.mapper, mapper)
	}
	return n
}

func (n *JobNode) AddReducer(reducer mapred.Reducer, num int) *JobNode {
	if num <= 0 {
		num = 1
	}
	if n.endNode.reducer != nil {
		mrnode := &MapReduceNode{
			index:           n.graph.mrNodeIndex,
			jobNode:         n,
			mapper:          IdentityMapper,
			reducer:         reducer,
			chainPrev:       n.endNode,
			inputFiles:      n.endNode.outputFiles,
			mapperBatchSize: 1,
		}
		mrnode.interFiles.mrNode = mrnode
		n.graph.mrNodeIndex++
		n.endNode.chainNext = mrnode
		n.endNode = mrnode
		n.graph.mrNodes = append(n.graph.mrNodes, mrnode)
	} else {
		//use origin
		n.endNode.reducer = reducer
	}
	n.endNode.outputFiles = &fileNameGenerator{n.endNode, num}
	n.endNode.reducerCount = num
	return n
}

func (n *JobNode) SetName(name string) *JobNode {
	n.name = name
	return n
}

func (n *JobNode) DependOn(nodes ...*JobNode) *JobNode {
	n.dependencies = append(n.dependencies, nodes...)
	for _, n := range nodes {
		n.dependencyOf = append(n.dependencyOf, n)
	}
	return n
}

func (n *JobNode) GetMapReduceNodes() (res []*MapReduceNode) {
	res = make([]*MapReduceNode, 0)
	for startNode := n.startNode; startNode != nil; startNode = startNode.chainNext {
		res = append(res, startNode)
	}
	return
}

// GetDependencies return a copy of
func (n *JobNode) GetDependencies() (res []*JobNode){
	res = make([]*JobNode, len(n.dependencies))
	copy(res, n.dependencies)
	return
}

// GetDependencyOf return a copy of
func (n *JobNode) GetDependencyOf() (res []*JobNode){
	res = make([]*JobNode, len(n.dependencyOf))
	copy(res, n.dependencyOf)
	return
}

func (j *Job) AddMapper(mapper mapred.Mapper, inputs Files, batchSize ...int) *JobNode {
	if len(batchSize) == 0 {
		batchSize = append(batchSize, 1)
	}
	jnode := &JobNode{
		graph: j,
	}
	mrnode := &MapReduceNode{
		index:           j.mrNodeIndex,
		jobNode:         jnode,
		mapper:          mapper,
		inputFiles:      inputs,
		mapperBatchSize: batchSize[0],
	}
	j.mrNodeIndex++
	jnode.startNode = mrnode
	jnode.endNode = mrnode
	mrnode.outputFiles = &fileNameGenerator{mrnode, 0}
	mrnode.interFiles.mrNode = mrnode

	j.roots = append(j.roots, jnode)
	j.mrNodes = append(j.mrNodes, mrnode)
	return jnode
}

func (j *Job) AddReducer(reducer mapred.Reducer, inputs Files, num int) *JobNode {
	if num <= 0 {
		num = 1
	}
	jnode := &JobNode{
		graph: j,
	}
	mrnode := &MapReduceNode{
		index:           j.mrNodeIndex,
		jobNode:         jnode,
		mapper:          IdentityMapper,
		reducer:         reducer,
		inputFiles:      inputs,
		mapperBatchSize: 1,
	}
	j.mrNodeIndex++
	jnode.startNode = mrnode
	jnode.endNode = mrnode
	mrnode.outputFiles = &fileNameGenerator{mrnode, num}
	mrnode.interFiles.mrNode = mrnode
	mrnode.reducerCount = num

	j.roots = append(j.roots, jnode)
	j.mrNodes = append(j.mrNodes, mrnode)
	return jnode
}

// ValidateGraph validate the graph to ensure it can be excuted
func (j *Job) ValidateGraph() {
	visitedMap := make(map[*JobNode]bool)
	jobNodeNameMap := make(map[string]bool)
	mrNodeIndexMap := make(map[int]bool)
	mapredNodeCount := 0
	nodeStack := util.Stack{}
	var dfs func(*JobNode)
	dfs = func(node *JobNode) {
		if node.startNode == nil {
			log.Fatal("Start node in a job node should not be nil. Job:", node.name)
		}
		for _, n := range nodeStack.Items() {
			if node.startNode.index == n.(int) {
				log.Fatal("Job graph has circle around", node.name)
			}
		}
		if _, ok := visitedMap[node]; ok {
			return
		}
		visitedMap[node] = true
		if _, ok := jobNodeNameMap[node.name]; ok {
			log.Fatal("Duplicate job name", node.name)
		}
		jobNodeNameMap[node.name] = true
		// Check whether map/reduce chain is correct
		for startNode := node.startNode; startNode != nil; startNode = startNode.chainNext {
			mapredNodeCount++

			if _, ok := mrNodeIndexMap[startNode.index]; ok {
				log.Fatalf("Duplicate MapReduceNode index %v in job %v", startNode.index, node.name)
			}
			mrNodeIndexMap[startNode.index] = true

			if len(startNode.inputFiles.GetFiles()) == 0 {
				log.Fatalf("%v-%v input file length is 0", node.name, startNode.index)
			}
			if len(startNode.outputFiles.GetFiles()) == 0 {
				log.Fatalf("%v-%v output file length is 0", node.name, startNode.index)
			}
			if startNode.chainPrev != nil && startNode.inputFiles != startNode.chainPrev.outputFiles {
				log.Fatalf("%v-%v input files doesn't equal to prev node output files", node.name, startNode.index)
			}
			if startNode.reducerCount == 0 {
				log.Fatalf("%v-%v reducer count is 0", node.name, startNode.index)
			}
			if startNode.mapperBatchSize == 0 {
				log.Fatalf("%v-%v mapper batch size is 0", node.name, startNode.index)
			}
			nMappers := len(startNode.inputFiles.GetFiles())
			if len(startNode.interFiles.getMapperOutputFiles(0))*
				len(startNode.interFiles.getReducerInputFiles(0)) != nMappers*startNode.reducerCount {
				log.Fatalf("%v-%v inter file len is not right 0", node.name, startNode.index)
			}
			if startNode.mapper == nil {
				log.Fatalf("%v-%v doesn't have mapper", node.name, startNode.index)
			}
			if startNode.reducer == nil {
				log.Fatalf("%v-%v doesn't have reducer", node.name, startNode.index)
			}
		}
		nodeStack.Push(int(node.startNode.index))
		for _, dep := range node.dependencyOf {
			dfs(dep)
		}
		nodeStack.Pop()
	}

	for _, n := range j.roots {
		dfs(n)
	}

	if mapredNodeCount != len(j.mrNodes) {
		log.Fatal("There is some orphan mapred node in the graph")
	}
}

func (j *Job) GetMapReduceNode(jobNodeName string, mapredIndex int) *MapReduceNode {
	for _, n := range j.mrNodes {
		if n.jobNode.name == jobNodeName && n.index == mapredIndex {
			return n
		}
	}
	return nil
}

func (j *Job) SetName(name string) {
	for _, c := range []rune(name) {
		if !unicode.IsLower(c) || c == rune('-') {
			log.Fatal("Job name should only contain lowercase and '-'")
		}
	}
	j.name = name
}

func (j *Job) GetRootNodes() (res []*JobNode){
	res = make([]*JobNode, len(j.roots))
	copy(res, j.roots)
	return
}
