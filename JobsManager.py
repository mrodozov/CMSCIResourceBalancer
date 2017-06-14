__author__ = 'mrodozov@cern.ch'
'''
This class instance(singleton) manages jobs ordering
'''

import json
from Singleton import Singleton
from threading import Lock, Thread
from time import sleep
from operator import itemgetter

class JobsManager(object):

    __metaclass__ = Singleton

    def __init__(self, jobs=None):
        self.jobs = jobs
        self.results = None
        self.jobs_lock = Lock() # lock when touching jobs structure
        self.results_lock = Lock() # lock when touching results structure

    def getNextJobs(self, sort_function=None):
        next_jobs = []
        with self.jobs_lock:
            for i in self.jobs:
                if not self.jobs[i].keys():
                    continue
                current_step = sorted( self.jobs[i].keys() )[0]
                cumulative_time = sum([self.jobs[i][j]['avg_time'] for j in self.jobs[i]])
                element = (i, current_step, cumulative_time, self.jobs[i][current_step]['avg_time'],
                           self.jobs[i][current_step]['avg_mem'], self.jobs[i][current_step]['commands'])
                next_jobs.append(element)
                #print i, j, self.jobs[i][j]['avg_time']

        return sorted(next_jobs, key=itemgetter(2), reverse=True)

    def removeJobFromMatrix(self, jobID=None, stepID=None, recursive=False):
        with self.jobs_lock:
            if recursive and jobID in self.jobs:
                del self.jobs[jobID]
            if jobID in self.jobs and stepID in self.jobs[jobID]:
                del self.jobs[jobID][stepID]

    def _insertRecordInResults(self, result=None):
        with self.results_lock:
            pass


''' 
the task list 
'''

if __name__ == "__main__":

    mm = JobsManager()
