__author__ = 'mrodozov@cern.ch'
'''
This class instance(singleton) manages jobs ordering
'''

import json
from Singleton import Singleton
from threading import Lock, Thread
from time import sleep
from operator import itemgetter
import psutil



def relval_test_process(job=None):
    # unpack the job and execute
    #jobID, jobStep, jobCumulativeTime, jobSelfTime, jobCommands = job.items()
    jobID = job[0]
    jobStep = job[1]
    jobCumulativeTime = job[2]
    jobSelfTime = job[3]
    jobCommands = job[4]
    jobSelfTime = jobSelfTime / 10

    while jobSelfTime:
        print 'eta: ', jobID, jobStep, jobSelfTime
        jobSelfTime = jobSelfTime - 1
        sleep(1)
    return {'id':jobID, 'step':jobStep, 'exit_code': 0}

class dummyThread(Thread):

    def __init__(self, target, *args):
        super(dummyThread, self).__init__()
        self._target = target
        self._args = args
        #self.name = str(args[0] + ' ' + args[1])
        self.resultQueue = None

    def run(self):
        result = self._target(*self._args)
        #put the result when the task is finished
        #result = result+' '+self.name
        self.resultQueue.put(result)

class JobsManager(object):

    __metaclass__ = Singleton

    def __init__(self, jobs=None):
        self.jobs = jobs
        self.started_jobs = None
        self.results = {}
        self.availableMemory = psutil.virtual_memory()[1]
        self.jobs_lock = Lock() # lock when touching jobs structure
        self.started_jobs_lock = Lock()
        self.results_lock = Lock() # lock when touching results structure

        ''' 
        add the thread jobs that put jobs on execution queue
        and finilizes them here
        '''
        self.started_jobs = [] # jobs already started
        self.putSelectedJobsOnQueue = Thread(target=self.putJobsOnQueueAndGetResults)
        self.toProcessQueue = None
        self.processedQueue = None

    '''
    methods to check resources availability
    '''

    def checkIfEnoughMemory(self, mem_value=0):
        return self.availableMemory > mem_value
        #or use a record of the remaining memory
    '''
    methods executed as separated threads
    '''

    def putJobsOnQueueAndGetResults(self):

        while True:

            #get jobs from the structure put them on queue to process
            next_jobs = self.getNextJobs()
            print 'getting next jobs:'

            for j in next_jobs:
                print j[0], j[1]

            for job in next_jobs:
                if job[0] in self.started_jobs or not self.checkIfEnoughMemory(job[4]):
                    print 'skipping job', job[0], job[1]
                    continue
                with self.started_jobs_lock:
                    self.started_jobs.append(job[0])
                    self.availableMemory = self.availableMemory - job[4]
                thread_job = dummyThread(relval_test_process, job)
                self.toProcessQueue.put(thread_job)


            if self.jobs:
                print 'there are jobs'
                print self.jobs


            if self.toProcessQueue.empty() and self.processedQueue.empty() and not self.jobs:
                #all done
                print 'two queues'
                break

            #get jobs from processed queue, finish them and write the result status
            finishedJob = self.processedQueue.get()
            self.finishJob(finishedJob)




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

    def _removeJobFromMatrix(self, jobID=None, stepID=None, recursive=False):
        with self.jobs_lock:
            if jobID in self.jobs and (recursive or len(self.jobs[jobID])==1):
                del self.jobs[jobID]
            if jobID in self.jobs and stepID in self.jobs[jobID]:
                del self.jobs[jobID][stepID]

    def _insertRecordInResults(self, result=None):
        with self.results_lock:
            self.results.update(result)

    def finishJob(self, job=None):
        print 'finish'
        self._removeJobFromMatrix(job['id'], job['step'], job['exit_code'] is not 0)
        with self.started_jobs_lock:
            self.started_jobs.remove(job['id'])
        self._insertRecordInResults(job)


''' 
the task list 
'''

if __name__ == "__main__":



    pass






