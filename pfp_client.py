import sys
import os
import uuid
import paramiko
import common
import time
import shutil
from multiprocessing.managers import BaseManager,SyncManager
from multiprocessing import Queue,Process
import traceback

class ServerQueueManager(SyncManager):
        pass

def make_client_manager(ip,port,auth_key):
    """ Create a manager for a client. This manager connects to a server on the
        given address and exposes the get_job_q and get_result_q methods for
        accessing the shared queues from the server.
        Return a manager object.
    """
    
    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')
    
    manager = ServerQueueManager(address=(ip,port),authkey=auth_key)
    manager.connect()
    
    print 'Client connected to %s:%s' % (ip, port)
    return manager

def robust_sftp(sftp, remote, local, get_flag):
    try_counts = 0
    while try_counts < 3:
        try_counts += 1
        try:
            if get_flag:
                sftp.get(remote, local)
            else:
                sftp.put(local,remote)        
            return True
        except Exception, e:
            print traceback.format_exc()
    return False


def do_job_worker(job_q, result_q, sftp):
    print 'process %d start working...'%(os.getpid())
    #construct feature file name base on MAC address and process id
    mac=uuid.UUID(int = uuid.getnode()).hex[-12:]
    feature_file_name = 'feature_%s_%d.tbl' %(mac,os.getpid())
    remote_feature_file = ''
    task_feature = {}
    while True:
        try:
            job = job_q.get_nowait()
            print 'will process %d jobs..., pid=%d'%(len(job),os.getpid())
            for remote_files in job:
                if robust_sftp(sftp, remote_fet_file, local_fet_file, True) == False:
                    continue
                try:
                    process_your_file
                    robust_sftp(sftp, remote_fet_file, local_fet_file, False)
                    
                except Exception,ex:
                    print traceback.format_exc()
                finally:
                    os.remove(local_fet_file)
            result_q.put(len(job))
        except Exception,e:
            if job_q.empty():
                print 'Queue.Empty, will exit loop. pid=%d'%(os.getpid())
                break
            else:
                print 'Is not job_q.empty() exception, pid=%d'%(os.getpid())
                print traceback.format_exc()

    print 'process %d exit.'%(os.getpid())

def process_file_worker(job_q, result_q, server_ip, operation, user_name):
    """ A worker function to be launched in a separate process. Takes jobs from
        job_q - each job is a list of files(one chunk of files) to process. When the job is done,
        the result (number of files processed) is placed into
        result_q. Runs until job_q is empty.
    """
    scp = paramiko.Transport((server_ip, 22))
    scp.connect(username=user_name,password=common.g_server_password)
    sftp = paramiko.SFTPClient.from_transport(scp)
    do_job_worker(job_q,result_q,sftp) 
    scp.close()


def mp_process_file(shared_job_q, shared_result_q, server_ip, nprocs,operation,user_name):
    """ Split the work with jobs in shared_job_q and results in
        shared_result_q into several processes. Launch each process with
        process_file_worker as the worker function, and wait until all are
        finished.
    """
    procs = []
    for i in range(nprocs):
        p = Process(
                target=process_file_worker,
                args=(shared_job_q, shared_result_q, server_ip, operation, user_name))
        procs.append(p)
        p.start()

    for p in procs:
        p.join()
        print 'successfully joined: %d'%(p.pid)


def run_client():
    if len(sys.argv) != 5:
        print 'Usage:'
        print sys.argv[0],' operation server_ip listen_port user_name'
        print 'supported operations: XXX'
        sys.exit()

    operation, server_ip, user_name = sys.argv[1], sys.argv[2], sys.argv[3]
    if not os.path.exists(LOG_CACHE_DIR):
        os.mkdir(LOG_CACHE_DIR)     
    try:
        mp_process_file(job_q, result_q, server_ip, num_procs, operation, user_name)
        os.rmdir(LOG_CACHE_DIR) 
    except:
        pass
    print 'client exit.'

if __name__ == '__main__':
    run_client()            
