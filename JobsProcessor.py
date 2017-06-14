__author__ = 'mrodozov@cern.ch'
'''

'''

from threading import Thread
import Queue
from time import sleep

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

class JobsProcessor(Thread):

    def __init__(self, to_process_queue=None, processed_queue=None):
        super(JobsProcessor, self).__init__()
        self._toProcessQueue = to_process_queue
        self._processedQueue = processed_queue
        self.startProcess = Thread(target=self.startTasks)
        self.finishProcess = Thread(target=self.finishTasks)

    def startTasks(self):
        while True:
            task = self._toProcessQueue.get()
            task.resultQueue = self._processedQueue
            task.start()
            print 'processing task ', task.name
            self._toProcessQueue.task_done()
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