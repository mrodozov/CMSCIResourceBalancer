from elasticsearch import Elasticsearch
import json


if __name__ == "__main__":

    query_url='http://cmses-master01.cern.ch:9200/ib-dataset-*/_search' # or 'http://cmses-master01.cern.ch:9200' ?
    base_url = 'http://cmses-master01.cern.ch:9200'
    uname = 'kibana'
    psswd = 'kibana'

    esclient = Elasticsearch([base_url])
    response = esclient.search(
        index='ib-dataset-*',

                               )

    print 'bla'