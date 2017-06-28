__author__ = 'mrodozov@cern.ch'
'''
this class instances are responsible to construct the jobs structure (json object) given list of workflows as input
for this the class has to 
1. run runTheMatrix in dummy mode
2. get history data from elastic search
3. construct the object and pass it to whoever needs to use it
'''

import json
from Singleton import Singleton
from es_utils import get_payload
from time import time
import subprocess
import os

class JobsConstructor(object):

    __metaclass__ = Singleton

    def __init__(self, workflows_list=None):

        self._workflows = workflows_list

    def _format(self, s, **kwds):
        return s % kwds

    def getWorkflowStatsFromES(self, release='*', arch='*', lastNdays=7, page_size=0):

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

        if page_size < 1:
            info_request = True
            queryInfo["page_size"] = 2
        else:
            queryInfo["page_size"] = page_size

        total_hits = 0

        while True:
            queryInfo["from"] = ent_from
            es_data = get_payload(query_url, self._format(query_datsets, **queryInfo))  # here
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
            if ent_from >= total_hits:
                break

        return json_out[0]['hits']['hits']

    def getJobsCommands(self, workflow_matrix_list=None, workflows_limit=None):
        #run runTheMatrix and parse the output for each workflow, example results structure in resources/wf.json
        #for now, get it from the file resources/wf.json
        #run_matrix_process = subprocess.Popen('runTheMatrix.py -l '+workflow_matrix_list+' -i all --maxSteps=0',
        #                                      shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        #                                      close_fds=True)
        #stdout, stderr = run_matrix_process.communicate()
        wf_base_folder = '/home/cmsbld/mrodozov/testScheduler/CMSCIResourceBalancer/'
        wf_folders = [fld for fld in os.listdir(wf_base_folder) if os.path.isdir(wf_base_folder+fld)]
        #print os.listdir(wf_base_folder)
        matrix_map = {}
        #print wf_folders
        counter = 0
        for f in wf_folders:
            #print f
            wf_id = f.split('_')[0]
            if not os.path.exists(os.path.join(wf_base_folder, f, 'wf_steps.txt')):
                continue
            matrix_map[wf_id] = {}
            with open(os.path.join(wf_base_folder, f, 'wf_steps.txt')) as wf_file:
                for line in wf_file.readlines():
                    stepID, stepCommands = line.split(':',1)
                    #print stepID
                    #print stepCommands
                    matrix_map[wf_id][stepID] = {'description':[], 'commands': stepCommands}
            counter += 1
            if workflows_limit and counter > workflows_limit:
                break
            #print wf_id
        #print json.dumps(matrix_map, indent=1, sort_keys=True)
        #
        #with open('resources/wf.json') as matrixFile:
        #    matrixMap = json.loads(matrixFile.read())
        return matrix_map


    def constructJobsMatrix(self, release, arch, days, page_size, workflow_matrix_list, wf_limit):
        jobs_ids_and_commands = self.getJobsCommands(workflow_matrix_list, wf_limit)
        jobs_stats = self.getWorkflowStatsFromES(release, arch, days, page_size)
        #for local test get the stats from a file
        #with open('resources/exampleESqueryResult.json') as esQueryFromFile:
        #    jobs_stats = json.loads(esQueryFromFile.read())[0]['hits']['hits']

        ESworkflowsData = jobs_stats
        matrixMap = jobs_ids_and_commands

        for i in ESworkflowsData:
            if i['_source']['workflow'] in matrixMap and i['_source']['step'] in matrixMap[i['_source']['workflow']]:
                matrixMap[i['_source']['workflow']][i['_source']['step']]['description'].append(i['_source'])

        # print json.dumps(matrixMap, indent=2, sort_keys=True, separators=(',', ': ')) # GOOD Anakin, GOOOOOOD

        for wf_id in matrixMap:
            for step_id in matrixMap[wf_id]:
                nKeys = len(matrixMap[wf_id][step_id]['description'])
                countTime = 0
                countMem = 0
                for rec in matrixMap[wf_id][step_id]['description']:
                    countTime += int(rec['time'])
                    countMem += int(rec['rss_avg'])
                if nKeys > 0:
                    matrixMap[wf_id][step_id]['avg_time'] = countTime / nKeys
                    matrixMap[wf_id][step_id]['avg_mem'] = countMem / nKeys
                else:
                    matrixMap[wf_id][step_id]['avg_time'] = 21600
                    matrixMap[wf_id][step_id]['avg_mem'] = 4500000000

        return matrixMap

if __name__ == "__main__":

    opts = None
    release = 'CMSSW_9_2_X*'
    arch = 'slc6_amd64_gcc530'
    days = 7
    page_size = 0

    wf_list = None
    with open('resources/wf_slc6_530.txt') as wf_list_file:
        wf_list = wf_list_file.read().replace('\n', ',')
        wf_list = wf_list[:-1]


    jc = JobsConstructor(wf_list)
    jc.getJobsCommands(wf_list)
    
    #print wf_list

    limit = 20

    json_out = jc.constructJobsMatrix(release, arch, days, page_size, wf_list, limit)
    print json.dumps(json_out, indent=2, sort_keys=True, separators=(',', ': '))
    print len(json_out)
