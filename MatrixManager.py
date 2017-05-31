'''
This class instances will make the
'''
import json
from Singleton import Singleton
from RelvalProcessProfile import ProcessProfile as PProfile

class MatrixManager(object):

    __metaclass__ = Singleton

    def __init__(self):
        self.processesList = None


if __name__ == "__main__":

    mm = MatrixManager()
    mm.processesList = {'':''}

    pass
