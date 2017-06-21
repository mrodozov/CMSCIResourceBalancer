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
from JobsConstructor import JobsConstructor
from JobsManager import JobsManager, relval_test_process, dummyThread
from JobsProcessor import JobsProcessor
from threading import Thread, Event
import Queue
from optparse import OptionParser


if __name__ == "__main__":

    parser = OptionParser(usage="%prog ")
    parser.add_option("-r", "--release", dest="release", help="Release filter", type=str, default="*")
    parser.add_option("-a", "--architecture", dest="arch",
                      help="SCRAM_ARCH filter. Production arch for a release cycle is used if found otherwise slc6_amd64_gcc530",
                      type=str, default=None)
    parser.add_option("-d", "--days", dest="days", help="Files access in last n days", type=int, default=7)
    parser.add_option("-j", "--job", dest="job", help="Parallel jobs to run", type=int, default=4)
    parser.add_option("-p", "--page", dest="page_size",
                      help="Page size, default 0 means no page and get all data in one go", type=int, default=0)
    opts, args = parser.parse_args()

    if not opts.arch:
        if opts.release == "*":
            opts.arch = "*"
        else:
            script_path = abspath(dirname(argv[0]))
            err, out = getstatusoutput(
                "grep 'RELEASE_QUEUE=%s;' %s/config.map | grep -v 'DISABLED=1;' | grep 'PROD_ARCH=1;' | tr ';' '\n' | grep 'SCRAM_ARCH=' | sed 's|.*=||'" % (
                opts.release, script_path))
            if err:
                opts.arch = "slc6_amd64_gcc530"
            else:
                opts.arch = out
    if opts.release != "*": opts.release = opts.release + "*"

    opts.release = 'CMSSW_9_2_X*'
    opts.arch = 'slc6_amd64_gcc530'
    opts.days = 7
    opts.page_size = 0

    ''' here the program is tested  '''

    toProcessQueue = Queue.Queue()
    processedTasksQueue = Queue.Queue()

    jc = JobsConstructor()
    matrixMap =jc.constructJobsMatrix(opts.release, opts.arch, opts.days, opts.page_size, None)

    jm = JobsManager(matrixMap)
    jm.toProcessQueue = toProcessQueue
    jm.processedQueue = processedTasksQueue

    getNextJobsEvent = Event()
    finishJobsEvent = Event()

    jp = JobsProcessor(toProcessQueue, processedTasksQueue)
    jp.allJobs = jm.jobs
    jp.allJobs_lock = jm.jobs_lock

    jm.getNextJobsEvent = getNextJobsEvent
    jm.finishJobsEvent = finishJobsEvent

    jm.putJobsOnProcessQueue.start()
    jm.getProcessedJobs.start()
    jp.start()

    jm.putJobsOnProcessQueue.join()
    jm.getProcessedJobs.join()
    jp.join()





