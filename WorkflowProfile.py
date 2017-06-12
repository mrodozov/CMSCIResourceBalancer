import json
import sys

class WorkflowObject(object):

    def __init__(self, description = None):
        #description
        try:
            self.id = description['id']
            self.usedMemory = description['usedMemory']
            self.duration = description['duration']
            self.command = description['command']
        except KeyError as ne:
            print 'missing key for process description object, ', ne.message
            sys.exit(-1)

if __name__ == "__main__":

    description_object = {'id':'bla', 'usedMemory':'bla', 'duration':'bla'}

    pp = WorkflowObject(description_object)


