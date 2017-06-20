__author__ = 'mrodozov@cern.ch'
'''

'''

from threading import Thread, Event, Condition
import Queue
from time import sleep

def dummyTimeWastingTask(duration=10):
    while duration:
        print 'ETA: ', duration
        duration = duration -1
        sleep(1)
    return 'Exit code and whatever'

def relval_test_process(job=None):
    # unpack the job and execute
    #jobID, jobStep, jobCumulativeTime, jobSelfTime, jobCommands = job.items()
    jobID = job[0]
    jobStep = job[1]
    jobCumulativeTime = job[2]
    jobSelfTime = job[3]
    jobMem = job[4]
    jobCommands = job[5]
    jobSelfTime = 20 / 10

    while jobSelfTime:
        #print 'eta: ', jobID, jobStep, jobSelfTime
        sleep(1)
        jobSelfTime = jobSelfTime - 1

    return {'id': jobID, 'step': jobStep, 'exit_code': 0, 'mem': int(jobMem)}

class dummyThread(Thread):

    def __init__(self, target, *args):
        super(dummyThread, self).__init__()
        self._target = target
        self._args = args
        self.name = str(args[0] + ' ' + args[1])
        self.resultQueue = None

    def run(self):
        result = self._target(*self._args)
        print 'result is: ', result, '\n'
        #put the result when the task is finished
        #result = result+' '+self.name
        self.resultQueue.put(result)

class JobsProcessor(Thread):

    def __init__(self, to_process_queue=None, processed_queue=None):
        super(JobsProcessor, self).__init__()
        self._toProcessQueue = to_process_queue
        self._processedQueue = processed_queue
        self.allJobs = None
        self.startProcess = Thread(target=self.startTasks)
        self.finishProcess = Thread(target=self.finishTasks)

    def startTasks(self):
        while True:

            task = self._toProcessQueue.get()
            task.resultQueue = self._processedQueue
            task.start()
            #print 'processing task ', task.name
            self._toProcessQueue.task_done()
            #self.finish
            print 'size of jobs:', len(self.allJobs), "\n"
                #, self.allJobs
            if not self.allJobs:
                print 'finished get queue'
                break


    def finishTasks(self):
        while True:
            result = self._processedQueue.get()
            print 'finishing task', result['id'], result['step']
            if self._processedQueue.empty() and not self.allJobs:
                print 'finished put queue'
                break

    def run(self):

        self.startProcess.start()
        #self.finishProcess.start()
        self.startProcess.join()
        print ''
        #self.finishProcess.join()


if __name__ == "__main__":

    toProcessQueue = Queue.Queue()
    processedTasksQueue = Queue.Queue()

    dthread1 = dummyThread(dummyTimeWastingTask, 10)
    dthread1.name = 'first'
    dthread2 = dummyThread(dummyTimeWastingTask, 15)
    dthread2.name = 'second'
    dthread3 = dummyThread(dummyTimeWastingTask, 20)
    dthread3.name = 'third'

    toProcessQueue.put(dthread1)
    toProcessQueue.put(dthread2)
    toProcessQueue.put(dthread3)

    tp = JobsProcessor(toProcessQueue, processedTasksQueue)
    tp.start()
    tp.join()