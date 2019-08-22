import time
import pdb
import mock_db
import uuid
from worker import worker_main
from threading import Thread

def lock_is_free():
    """
        CHANGE ME, POSSIBLY MY ARGS

        Return whether the lock is free
    """
    locked = db.find_one({"Locked": True})
    if locked:
        return False
    return True


def attempt_run_worker(worker_hash, give_up_after, db, retry_interval):
    """
        CHANGE MY IMPLEMENTATION, BUT NOT FUNCTION SIGNATURE

        Run the worker from worker.py by calling worker_main

        Args:
            worker_hash: a random string we will use as an id for the running worker
            give_up_after: if the worker has not run after this many seconds, give up
            db: an instance of MockDB
            retry_interval: continually poll the locking system after this many seconds
                            until the lock is free, unless we have been trying for more
                            than give_up_after seconds
    """

    worker = db.find_one({"_id": worker_hash})
    if not worker:
        worker = db.insert_one({"_id": worker_hash, "Locked": False, "time_stamp": time.time()})
    service_healthy = True

    while True:
        worker = db.find_one({"_id": worker_hash})
        current_time = time.time()
        if (current_time - worker["time_stamp"]) > give_up_after:
            print("\n\n    ### Worker {} timed out. Stopping worker {}... ###\n\n".format(worker_hash, worker_hash))
            return
        if lock_is_free() and service_healthy:
            try:
                worker = db.update_one({"_id": worker_hash}, {"_id": worker_hash, "Locked": True, "time_stamp": current_time})
                print("Lock is free, worker {} has aquired lock.".format(worker_hash))
                worker_main(worker_hash, db)
                print("\n\n    ### Worker {} completed recurring task. ###\n\n".format(worker_hash))
            except:
                print("\n\n    ### Worker {} has failed. Worker will poll unsuccessfully until timeout... ###\n\n".format(worker_hash, worker_hash))
                service_healthy = False
            finally:
                # create new time_stamp simulating perhaps it was a long job to complete.
                db.update_one({"_id": worker_hash}, {"_id": worker_hash, "Locked": False, "time_stamp": time.time()})
                print("worker {} has released lock.".format(worker_hash))
        else:
            time.sleep(retry_interval)
    return

if __name__ == "__main__":
    """
        DO NOT MODIFY

        Main function that runs the worker five times, each on a new thread
        We have provided hard-coded values for how often the worker should retry
        grabbing lock and when it should give up. Use these as you see fit, but
        you should not need to change them
    """

    db = mock_db.DB()
    threads = []
    for _ in range(5):
        t = Thread(target=attempt_run_worker, args=(uuid.uuid1(), 2000, db, 5))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
