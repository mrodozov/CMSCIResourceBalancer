__author__ = 'mrodozov@cern.ch'
'''
This class instance(singleton) manages jobs ordering
'''

from Singleton import Singleton
from threading import Lock, Thread
from time import sleep
from operator import itemgetter
import psutil

'''
method putNextJobsOnQueue may need to use another lock
whenever it starts to prevent method getFinishedJobs while loop to
get called more than once while putNextJobsOnQueue is still
executing 

'''


def relval_test_process(job=None):
    # unpack the job and execute
    #jobID, jobStep, jobCumulativeTime, jobSelfTime, jobCommands = job.items()
    jobID = job[0]
    jobStep = job[1]
    jobCumulativeTime = job[2]
    jobSelfTime = job[3]
    jobMem = job[4]
    jobCommands = job[5]
    jobSelfTime = 0.001

    while jobSelfTime > 0:
        #print 'eta: ', jobID, jobStep, jobSelfTime
        sleep(0.001)
        jobSelfTime = jobSelfTime - 0.001

    return {'id': jobID, 'step': jobStep, 'exit_code': 0, 'mem': int(jobMem)}

class dummyThread(Thread):

    def __init__(self, target, *args):
        super(dummyThread, self).__init__()
        self._target = target
        self._args = args
        #self.name = str(args[0] + ' ' + args[1])
        self.resultQueue = None
        self.getNextJobs = None

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
        self.putJobsOnProcessQueue = Thread(target=self.putJobsOnQueue)
        self.getProcessedJobs = Thread(target=self.getFinishedJobs)
        self.toProcessQueue = None
        self.processedQueue = None
        self.getNextJobsEvent = None


    '''
    methods to check resources availability
    '''

    def checkIfEnoughMemory(self, mem_value=0):
        return self.availableMemory > mem_value
        #or use a record of the remaining memory
    '''
    put jobs for processing methods
    '''

    def putJobsOnQueue(self):

        while True:
            if not self.jobs:
                print 'to process queue completed, breaking put jobs on queue', '\n'
                break

            #get jobs from the structure put them on queue to process
            next_jobs = self.getNextJobs()
            print 'put jobs on queue getting next jobs:', '\n'#, next_jobs
            self.putNextJobsOnQueue(next_jobs)
            self.getNextJobsEvent.wait()

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

    def putNextJobsOnQueue(self, jobs=None):
        print 'put next jobs on queue', '\n'
        for j in jobs:
            print j[0], j[1]

        for job in jobs:
            with self.started_jobs_lock:
                if job[0] in self.started_jobs or not self.checkIfEnoughMemory(job[4]):
                    print 'skipping job', job[0], job[1]
                    continue
                self.started_jobs.append(job[0])
                self.availableMemory = self.availableMemory - job[4]
                thread_job = dummyThread(relval_test_process, job)
                self.toProcessQueue.put(thread_job)
            self._removeJobFromWorkflow(job[0], job[1])
            #print self.jobs

        self.getNextJobsEvent.clear()
        print 'clearing'
        #self.finishJobsEvent.set()

    '''
    finishing jobs after process
    '''

    def getFinishedJobs(self):

        while True:

            print 'get finished jobs', '\n'
            #print 'jobs from finished jobs', '\n', self.jobs

            finishedJob = self.processedQueue.get()
            self.finishJob(finishedJob)
            #print finishedJob['id']
            self.processedQueue.task_done()
            self.getNextJobsEvent.set()

            print 'finished get finished jobs for ', finishedJob['id'], '\n'

            if not self.jobs and not self.started_jobs:
                print 'breaking get finished jobs'
                break

    def finishJob(self, job=None):
        print 'finish', job['id'], job['step'], job['exit_code'], job['mem']
        self.availableMemory += job['mem']
        with self.started_jobs_lock:
            self.started_jobs.remove(job['id'])
        self._insertRecordInResults(job)

    '''
    
    '''

    def _removeJobFromWorkflow(self, jobID=None, stepID=None):
        with self.jobs_lock:
            if jobID in self.jobs and stepID in self.jobs[jobID]:
                if len(self.jobs[jobID]) == 1:
                    self._removeWorkflow(jobID)
                else:
                    del self.jobs[jobID][stepID]

    def _removeWorkflow(self, wf_id=None):
        if wf_id in self.jobs:
            del self.jobs[wf_id]

    def _insertRecordInResults(self, result=None):
        with self.results_lock:
            self.results.update(result)





''' 
the task list 
'''

if __name__ == "__main__":


    pass






