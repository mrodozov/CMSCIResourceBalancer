'''
This class instances will make the
'''
import json
from Singleton import Singleton
from WorkflowProfile import WorkflowObject as wfo

class MatrixManager(object):

    __metaclass__ = Singleton

    def __init__(self):
        self.taskList = None
    def getTaskWithMemoryLimitation(self, limitation):
        pass

    def getNextTask(self):
        pass





''' 
the task list 
'''

if __name__ == "__main__":





    mm = MatrixManager()
    mm.taskList = {'':''}

