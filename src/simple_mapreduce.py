import multiprocessing
import collections
import itertools

class SimpleMapReduce(object):
    def __init__(self, map_func, reduce_func, num_of_workers=8):
        self.map_func = map_func
        self.reduce_func = reduce_func
        self.pool = multiprocessing.Pool(num_of_workers)

    def partition(self, mapped_values):
        ''' Organize the mapped values
        For now do nothing
        Return a list of tuple, of which each tuple is a key and value
        '''
        return mapped_values
        
    def __call__(self, inputs, chunksize=1):
        ''' Process the inputs through the given map and reduce functions

        input:
            an iterable containing the data to be processed
        '''
        map_responses = self.pool.map(self.map_func, inputs, chunksize=chunksize)
        partition_data = self.partition(itertools.chain(map_responses))
        reduced_values = self.pool.map(self.reduce_func, partition_data)

        return reduced_values




        


