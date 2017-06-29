__author__ = 'mrodozov@cern.ch'
'''
This class instance(singleton) manages jobs ordering
'''

from Singleton import Singleton
from threading import Lock, Thread, Semaphore
from time import sleep
from operator import itemgetter
import psutil
import json
import subprocess

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
    jobCPU = job[5]
    jobCommands = job[6]
    prevJobExit = job[7]
    jobSelfTime = 0.001

    while True:
        #print 'eta: ', jobID, jobStep, jobSelfTime
        sleep(jobSelfTime)
        jobSelfTime -= 0.001
        if 0 > jobSelfTime:
            print 'breaking'
            break

    return {'id': jobID, 'step': jobStep, 'exit_code': 0, 'mem': int(jobMem)}


def process_relval_workflow_step(job=None):
    # unpack the job and execute
    #jobID, jobStep, jobCumulativeTime, jobSelfTime, jobCommands = job.items()
    jobID = job[0]
    jobStep = job[1]
    jobCumulativeTime = job[2]
    jobSelfTime = job[3]
    jobMem = job[4]
    jobCPU = job[5]
    jobCommands = job[6]
    prevJobExit = job[7]
    jobCommands = 'ls'

    exit_code = 0

    if prevJobExit is not 0:
        return {'id': jobID, 'step': jobStep, 'exit_code': 'notRun', 'mem': int(jobMem), 'cpu': int(jobCPU),
                'stdout': 'notRun', 'stderr': 'notRun'}

    child_process = subprocess.Popen(jobCommands, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                     close_fds=True)
    stdout, stderr = child_process.communicate()
    exit_code = child_process.returncode
    #to test the non zero exit code

    return {'id': jobID, 'step': jobStep, 'exit_code': exit_code, 'mem': int(jobMem), 'cpu': int(jobCPU),
            'stdout': stdout, 'stderr': stderr}
    #start a subprocess, return it's output



def finilazeWorkflow(workflowID=None):

    pass


class workerThread(Thread):

    def __init__(self, target, *args):
        super(workerThread, self).__init__()
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
        self.availableMemory = None
        self.availableCPU = None
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
        self.counter = Semaphore()

    '''
    methods to check resources availability
    '''

    def checkIfEnoughMemory(self, mem_value=0):
        return self.availableMemory > mem_value
        #or use a record of the remaining memory

    def checkIfEnoughCPU(self, cpu_value=0):
        return self.availableCPU > cpu_value

    '''
    put jobs for processing methods
    '''

    def putJobsOnQueue(self):

        while True:
            if not self.jobs:
                print 'to process queue completed, breaking put jobs on queue', '\n'
                break

            #get jobs from the structure put them on queue to process
            self.counter.acquire() #acquire resource once, the finishing thread would release it for each finished job
            next_jobs = self.getNextJobs()
            print 'put jobs on queue getting next jobs:', '\n' #, next_jobs
            self.putNextJobsOnQueue(next_jobs)

    def getNextJobs(self, sort_function=None):
        next_jobs = []
        with self.jobs_lock:
            for i in self.jobs:

                if not self.jobs[i].keys():
                    continue
                '''
                check if the prev job had finished with exit != 0 and if it did, 
                '''

                prev_exit = 0
                with self.results_lock:
                    if i in self.results:
                        for s in self.results[i]:
                            if self.results[i][s]['exit_code'] is not 0:
                                prev_exit = self.results[i][s]['exit_code']
                                break

                current_step = sorted( self.jobs[i].keys() )[0]
                cumulative_time = sum([self.jobs[i][j]['avg_time'] for j in self.jobs[i]])
                element = (i, current_step, cumulative_time, self.jobs[i][current_step]['avg_time'],
                           self.jobs[i][current_step]['avg_mem'], self.jobs[i][current_step]['avg_cpu'],
                           self.jobs[i][current_step]['commands'], prev_exit)

                next_jobs.append(element)
                #print i, j, self.jobs[i][j]['avg_time']

        return sorted(next_jobs, key=itemgetter(2), reverse=True)

    def putNextJobsOnQueue(self, jobs=None):
        print 'put next jobs on queue', '\n'
        for j in jobs:
            print j[0], j[1]

        for job in jobs:

            if job[0] in self.started_jobs or not self.checkIfEnoughMemory(job[4]) or not self.checkIfEnoughCPU(job[5]):

                resource_not_available = []
                if job[0] in self.started_jobs:
                    resource_not_available.append('prev step not finished')
                if not self.checkIfEnoughMemory(job[4]):
                    resource_not_available.append('not enough memory')
                if not self.checkIfEnoughCPU(job[5]):
                    resource_not_available.append('not enough cpu')

                print 'skipping job', job[0], job[1], 'because ', ','.join(resource_not_available)

                continue

            with self.started_jobs_lock:
                self.started_jobs.append(job[0])
                self.availableMemory = self.availableMemory - job[4]
                self.availableCPU = self.availableCPU - job[5]
                thread_job = workerThread(process_relval_workflow_step, job)
                self.toProcessQueue.put(thread_job)
            self._removeJobFromWorkflow(job[0], job[1])
            #print self.jobs

        print 'jobs putted on queue'


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
            self.counter.release() # release the lock for each finished job, putJobsOnQueue retries to put new jobs


            print 'finished get finished jobs for ', finishedJob['id'], '\n'

            if not self.jobs and not self.started_jobs:
                print 'breaking get finished jobs'
                break

    def finishJob(self, job=None):
        print 'finish', job['id'], job['step'], job['exit_code'], job['mem'], job['cpu']

        self._insertRecordInResults(job)
        #insert the record before removing the job since it might remove the entire job

        with self.started_jobs_lock:
            self.availableMemory += job['mem']
            self.availableCPU += job['cpu']
            self.started_jobs.remove(job['id'])
            print 'job removed: ', job['id']

        #finish the workflow if the step was the last

        with self.jobs_lock:
            if not job['id'] in self.jobs:
                print 'finalize wf:', job['id']
                with self.results_lock:
                    finilazeWorkflow(job['id'])
                    self.results[job['id']]['finishing_exit'] = 'finished'
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
            if not result['id'] in self.results:
                self.results[result['id']] = {}

            self.results[result['id']][result['step']] = {'exit_code': result['exit_code'], 'stdout': result['stdout'],
                                                          'stderr':result['stderr']}


    def writeResultsInFile(self, file=None):
        with self.results_lock:
            with open(file, 'w') as results_file:
                results_file.write(json.dumps(self.results, indent=1, sort_keys=True))



''' 
the task list 
'''

if __name__ == "__main__":


    pass






