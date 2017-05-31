'''
this is just a wrapper for resources info checks.
'''

from Singleton import Singleton
import psutil


class ResourcePool(object):

    __metaclass__ = Singleton

    def __init__(self):
        pass

    def __del__(self):
        pass

    def getAvailableMemory(self):
        return psutil.virtual_memory()[1]



if __name__ == "__main__":

    print 'memory available'
    memory = psutil.virtual_memory()
    print psutil.cpu_freq()
    print psutil.cpu_stats()
    print memory

    for i in memory:
        print i

    res_pool = ResourcePool()