#!/usr/bin/python

from os.path import dirname, basename, join, exists, abspath
from sys import exit, argv
from time import time, sleep
import json
from commands import getstatusoutput
from es_utils import get_payload

def format(s, **kwds): return s % kwds

"""
"query": "release:%(release_cycle)s AND architecture:%(architecture)s AND workflow:561.0", -> thats the actual query
"""

query_url='http://cmses-master01.cern.ch:9200/relvals_stats_*/_search'

query_datsets = """
{
  "query": {
    "filtered": {
      "query": {
        "bool": {
          "should": [
            {
              "query_string": {
                "query": "release:%(release_cycle)s AND architecture:%(architecture)s ", 
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

if __name__ == "__main__":

  from optparse import OptionParser
  parser = OptionParser(usage="%prog ")
  parser.add_option("-r", "--release",      dest="release", help="Release filter",   type=str, default="*")
  parser.add_option("-a", "--architecture", dest="arch",    help="SCRAM_ARCH filter. Production arch for a release cycle is used if found otherwise slc6_amd64_gcc530",   type=str, default=None)
  parser.add_option("-d", "--days",         dest="days",    help="Files access in last n days",   type=int, default=7)
  parser.add_option("-j", "--job",          dest="job",     help="Parallel jobs to run",   type=int, default=4)
  parser.add_option("-p", "--page",         dest="page_size", help="Page size, default 0 means no page and get all data in one go",  type=int, default=0)
  opts, args = parser.parse_args()

  if not opts.arch:
    if opts.release=="*": opts.arch="*"
    else:
      script_path = abspath(dirname(argv[0]))
      err, out = getstatusoutput("grep 'RELEASE_QUEUE=%s;' %s/config.map | grep -v 'DISABLED=1;' | grep 'PROD_ARCH=1;' | tr ';' '\n' | grep 'SCRAM_ARCH=' | sed 's|.*=||'" % (opts.release, script_path))
      if err: opts.arch="slc6_amd64_gcc530"
      else: opts.arch=out
  if opts.release!="*": opts.release=opts.release+"*"

  datasets = {}
  ent_from = 0
  json_out = []
  info_request = False
  queryInfo={}

  queryInfo["end_time"] = int(time()*1000)
  queryInfo["start_time"] = queryInfo["end_time"]-int(86400*1000*opts.days)
  queryInfo["architecture"]=opts.arch
  queryInfo["release_cycle"]=opts.release
  queryInfo["from"] = 0

  if opts.page_size < 1:
    info_request = True
    queryInfo["page_size"]=2
  else:
    queryInfo["page_size"]=opts.page_size

  total_hits = 0


  while True:
    queryInfo["from"] = ent_from
    es_data = get_payload(query_url, format (query_datsets, **queryInfo)) # here
    content = json.loads(es_data)
    content.pop("_shards", None)
    total_hits = content['hits']['total']
    if info_request:
      info_request = False
      queryInfo["page_size"]=total_hits
      continue
    hits = len(content['hits']['hits'])
    if hits==0: break
    ent_from = ent_from + hits
    json_out.append(content)
    if ent_from>=total_hits: break

  #print json.dumps(json_out, indent=2, sort_keys=True, separators=(',', ': '))

  hits = json_out[0]

  print hits.keys()
  print hits['hits'].keys()
  #print hits['hits']['hits']

  useful_records = {}


  for i in json_out[0]['hits']['hits']:
      '''
      print type(i)
      print i.keys()
      print i['_source']
      print i['_type']
      print i['_id']
      '''
      id = i['_id']
      k = i['_source']
      useful_records[id] = {'duration': k['time'], 'timestamp': k['@timestamp'], 'workflow': k['workflow']}


  print type(hits)

  print json.dumps(useful_records, indent=2, sort_keys=True)


  #print query_datsets
  #es_data = get_payload(query_url, format (query_datsets, **queryInfo))
  #print json.dumps(es_data, indent=2, sort_keys=True)






