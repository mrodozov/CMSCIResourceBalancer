import json
import sys

class ProcessProfile(object):

    def __init__(self, description = None):
        #description
        try:
            self.id = description['id']
            self.usedMemory = ['usedMemory']
            self.duration = ['duration']
        except KeyError as ne:
            print 'missing key for process description object, ', ne.message
            sys.exit(-1)

if __name__ == "__main__":

    description_object = {'id':'bla', 'usedMemory':'bla', 'duration':'bla'}

    pp = ProcessProfile(description_object)

