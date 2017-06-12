'''
This file is for raw test anything
'''

from os.path import dirname, basename, join, exists, abspath
from sys import exit, argv
from time import time, sleep
import json
from commands import getstatusoutput
from es_utils import get_payload
import ResourcePool
import WorkflowProfile
import MatrixManager
import ESQueryManager
import multiprocessing as mp
from multiprocessing.pool import ThreadPool
from threading import Thread
import Queue
from optparse import OptionParser

toProcessQueue = Queue.Queue()
processedTasksQueue = Queue.Queue()

def dummyTimeWastingTask(duration=10):
    while duration:
        print 'ETA: ', duration
        duration = duration -1
        sleep(1)
    return 'Exit code and whatever'

class dummyThread(Thread):

    def __init__(self, target, *args):
        super(dummyThread, self).__init__()
        self._target = target
        self._args = args
        self.name = None
        self.resultQueue = None

    def run(self):
        result = self._target(*self._args)
        #put the result when the task is finished
        result = result+' '+self.name
        self.resultQueue.put(result)

class TaskProcessor(Thread):

    def __init__(self, to_process_queue=None, processed_queue=None):
        super(TaskProcessor, self).__init__()
        self._toProcessQueue = to_process_queue
        self._processedQueue = processed_queue
        self.startProcess = Thread(target=self.startTasks)
        self.finishProcess = Thread(target=self.finishTasks)

    def startTasks(self):
        while True:
            task = toProcessQueue.get()
            task.resultQueue = self._processedQueue
            task.start()
            print 'processing task ', task.name
            toProcessQueue.task_done()
            if self._toProcessQueue.empty():
                break

    def finishTasks(self):
        while True:
            result = self._processedQueue.get()
            print 'finishing task', result
            if self._processedQueue.empty():
                break

    def run(self):

        self.startProcess.start()
        self.finishProcess.start()
        self.startProcess.join()
        self.finishProcess.join()

def format(s, **kwds): return s % kwds


def getWorkflowStatsFromES(release='*', arch='*', lastNdays=7, page_size=0):

    query_url = 'http://cmses-master01.cern.ch:9200/relvals_stats_*/_search'

    query_datsets = """
    {
      "query": {
        "filtered": {
          "query": {
            "bool": {
              "should": [
                {
                  "query_string": {
                    "query": "release:%(release_cycle)s AND architecture:%(architecture)s", 
                    "lowercase_expanded_terms": false
                  }
                }
              ]
            }
          },
          "filter": {
            "bool": {
              "must": [
                {
                  "range": {
                    "@timestamp": {
                      "from": %(start_time)s,
                      "to": %(end_time)s
                    }
                  }
                }
              ]
            }
          }
        }
      },
      "from": %(from)s,
      "size": %(page_size)s
    }
    """
    datasets = {}
    ent_from = 0
    json_out = []
    info_request = False
    queryInfo = {}

    queryInfo["end_time"] = int(time() * 1000)
    queryInfo["start_time"] = queryInfo["end_time"] - int(86400 * 1000 * lastNdays)
    queryInfo["architecture"] = arch
    queryInfo["release_cycle"] = release
    queryInfo["from"] = 0

    if opts.page_size < 1:
        info_request = True
        queryInfo["page_size"] = 2
    else:
        queryInfo["page_size"] = page_size

    total_hits = 0

    while True:
        queryInfo["from"] = ent_from
        es_data = get_payload(query_url, format(query_datsets, **queryInfo))  # here
        content = json.loads(es_data)
        content.pop("_shards", None)
        total_hits = content['hits']['total']
        if info_request:
            info_request = False
            queryInfo["page_size"] = total_hits
            continue
        hits = len(content['hits']['hits'])
        if hits == 0: break
        ent_from = ent_from + hits
        json_out.append(content)
        if ent_from >= total_hits: break

    return json_out

def sortObjectKeysByInternalObjectFieldValue(objectWithKeys=None, internalOrderKey=None):
    # expecting
    pass

    #should return list of keys in order





if __name__ == "__main__":

    '''
    dthread1 = dummyThread(dummyTimeWastingTask, 10)
    dthread1.name = 'first'
    dthread2 = dummyThread(dummyTimeWastingTask, 15)
    dthread2.name = 'second'
    dthread3 = dummyThread(dummyTimeWastingTask, 20)
    dthread3.name = 'third'

    toProcessQueue.put(dthread1)
    toProcessQueue.put(dthread2)
    toProcessQueue.put(dthread3)

    tp = TaskProcessor(toProcessQueue,processedTasksQueue)
    tp.start()
    tp.join()
    '''

    parser = OptionParser(usage="%prog ")
    parser.add_option("-r", "--release", dest="release", help="Release filter", type=str, default="*")
    parser.add_option("-a", "--architecture", dest="arch",
                      help="SCRAM_ARCH filter. Production arch for a release cycle is used if found otherwise slc6_amd64_gcc530",
                      type=str, default=None)
    parser.add_option("-d", "--days", dest="days", help="Files access in last n days", type=int, default=7)
    parser.add_option("-j", "--job", dest="job", help="Parallel jobs to run", type=int, default=4)
    parser.add_option("-p", "--page", dest="page_size",
                      help="Page size, default 0 means no page and get all data in one go", type=int, default=0)
    opts, args = parser.parse_args()

    if not opts.arch:
        if opts.release == "*":
            opts.arch = "*"
        else:
            script_path = abspath(dirname(argv[0]))
            err, out = getstatusoutput(
                "grep 'RELEASE_QUEUE=%s;' %s/config.map | grep -v 'DISABLED=1;' | grep 'PROD_ARCH=1;' | tr ';' '\n' | grep 'SCRAM_ARCH=' | sed 's|.*=||'" % (
                opts.release, script_path))
            if err:
                opts.arch = "slc6_amd64_gcc530"
            else:
                opts.arch = out
    if opts.release != "*": opts.release = opts.release + "*"

    json_out = None
    #opts.release = 'CMSSW_9_2_X'
    opts.arch = 'slc6_amd64_gcc530'
    opts.days = 7
    opts.page_size = 0

    #json_out = getWorkflowStatsFromES(opts.release, opts.arch, opts.days, opts.page_size)
    # or get it from a file
    with open('resources/exampleESqueryResult.json') as esQueryFromFile:
        json_out = json.loads(esQueryFromFile.read())

    '''
    hits = json_out[0]
    useful_records = {}

    for i in json_out[0]['hits']['hits']:        
        #print type(i)
        #print i.keys()
        #print i['_source']
        #print i['_type']
        #print i['_id']        
        id = i['_id']
        k = i['_source']
        useful_records[id] = {'duration': k['time'], 'timestamp': k['@timestamp'], 'workflow': k['workflow']}
    
    #print json.dumps(useful_records, indent=2, sort_keys=True)
    '''

    #print json.dumps(json_out, indent=2, sort_keys=True, separators=(',', ': '))
    with open('resources/wf.json') as matrixFile:
        matrixMap = json.loads(matrixFile.read())
    #    print json.dumps(matrixMap, indent=1, sort_keys=True)

    # print query_datsets
    # es_data = get_payload(query_url, format (query_datsets, **queryInfo))
    # print json.dumps(es_data, indent=2, sort_keys=True)

    # draft the Workflow object, play with the way of extracting stats for workflow

    workFlowObject = {}

    ESworkflowsData = json_out[0]['hits']['hits']

    #for i in matrixMap:
    #    print i, len(matrixMap[i].keys()), matrixMap[i]

    for i in ESworkflowsData:
        if i['_source']['workflow'] in matrixMap and i['_source']['step'] in matrixMap[i['_source']['workflow']]:
            matrixMap[i['_source']['workflow']][i['_source']['step']]['description'].append(i['_source'])

    # print json.dumps(matrixMap, indent=2, sort_keys=True, separators=(',', ': ')) # GOOD Anakin, GOOOOOOD

    print sorted(matrixMap.keys())
    print len(matrixMap['2.0']['step1']['description'])

    for wf_id in matrixMap:
        for step_id in matrixMap[wf_id]:
            nKeys = len(matrixMap[wf_id][step_id]['description'])

            countTime = 0
            countMem = 0
            for rec in matrixMap[wf_id][step_id]['description']:
                countTime += int(rec['time'])
                countMem += int(rec['rss_avg'])
            matrixMap[wf_id][step_id]['avg_time'] = countTime / nKeys
            matrixMap[wf_id][step_id]['avg_mem'] = countMem / nKeys

    for i in matrixMap:
        for j in sorted(matrixMap[i]):
            print i, j, matrixMap[i][j]['avg_time']

    #TODO best to put the results into additional structure, and just remove the finished from the initial

    
