import os
import sys
import time
import shutil
import common
from multiprocessing.managers import BaseManager,SyncManager
from multiprocessing import Process, Queue

class JobQueueManager(SyncManager):
    pass

def do_process(shared_job_q,shared_result_q,top_src_dir,top_dst_dir,operation):
    file_list = []
    for root,dirs,files in os.walk(top_src_dir):
        for name in files:
            src_file = os.path.join(root,name)
            file_list.append(src_file)

    print '%d files to process...'%len(file_list)     
    chunk_size = common.g_chunk_size
    num_files = len(file_list)
    num_jobs = int(num_files / chunk_size)

    for i in range(0, num_jobs):
        shared_job_q.put(file_list[i * chunk_size : (i + 1) * chunk_size])

    if num_jobs * chunk_size < num_files:
        shared_job_q.put(file_list[num_jobs * chunk_size : num_files])

    if(shared_job_q.qsize() < 10):
        print 'job_q size is too small to run cluster'
        sys.exit()
    
    files_processed = 0
    while files_processed < num_files:
        files_processed += shared_result_q.get()
        print 'files processed: %d' % (files_processed)
    print 'all files processed!'

      
def make_server_manager(port,auth_key):
    """ Create a manager for the server, listening on the given port.
        Return a manager object with get_job_q and get_result_q methods.
    """
    job_q = Queue()
    result_q = Queue()
    # This is based on the examples in the official docs of multiprocessing.
    # get_{job|result}_q return synchronized proxies for the actual Queue
    # objects.
        
    JobQueueManager.register('get_job_q', callable=lambda: job_q)
    JobQueueManager.register('get_result_q', callable=lambda: result_q)
    manager = JobQueueManager(address=('',port), authkey=auth_key)
    manager.start()
    print 'Server started at port %s' %port
    return manager

 
if __name__ == '__main__':
    if len(sys.argv) != 5:
        print 'Usage:'
        print sys.argv[0], ' operation[...], ...'
        sys.exit()

    operation, dir_of_source, dir_of_target, listen_port = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])        
    manager = make_server_manager(listen_port, common.g_auth_key)
    do_process(manager.get_job_q(), manager.get_result_q(), dir_of_source, dir_of_target, operation)
    manager.shutdown()
        

   
