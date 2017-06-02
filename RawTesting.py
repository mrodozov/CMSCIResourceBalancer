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
import RelvalProcessProfile
import MatrixManager
import ESQueryManager
import multiprocessing as mp
from multiprocessing.pool import ThreadPool
from threading import Thread
import Queue

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

if __name__ == "__main__":

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
