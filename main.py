import logging
import threading

from collections import defaultdict
from math import inf
from time import sleep


class ThreadLogFilter(logging.Filter):
    def __init__(self, thread_names, *args, **kwargs):
        logging.Filter.__init__(self, *args, **kwargs)
        self.thread_names = thread_names

    def filter(self, record):
        return record.threadName in self.thread_names


log_format = '%(asctime)s: %(threadName)s %(message)s'
logging.basicConfig(format=log_format, level=logging.INFO, datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)
# logger.addFilter(ThreadLogFilter({'0', 'MainThread'}))


def queue_dict():
    return {
        'tasks': [],
        'last_v_finish': 0,
    }


queues = defaultdict(queue_dict)
same_weight_per_queue = 1
# lock = threading.Lock()  # TODO: protect shared `queues` being accessed from multiple threads


def active_queues():
    # backlogged sessions
    # session/flow/client/queue is backlogged if total length of backlogged traffic is more than 0
    # total length of backlogged traffic is difference between arrivals of the session (Ai)
    # and amount of service received by this session (Wi)
    # on opposite, sessions with empty queue are called absent
    return [q for q in queues.values() if q['tasks']]


def virtual_time_rate(number_of_workers):
    active = active_queues()
    if not active:
        rate = 0
    else:
        # wi(t) is normalized service received by session i during period (0, t]
        # it is equal to amount of service received divided by rate, allocated to the session
        # wi = Wi / allocated rate
        # in FFS normalized service received by backlogged sessions is equal
        # in WFQ vt rate is equal to wi of sessions belonging to busy period
        # ie vt increases in proportion to the normalized service received by any backlogged session
        rate = number_of_workers / (same_weight_per_queue * len(active))
        # rate is equal to:
        # output link rate (let's assume it is 1)
        # divided by sum of all rates allocated to backlogged sessions
    logger.debug('backlogged sessions %s, rate %s', len(active), rate)
    return rate


def virtual_time(amount_of_job_done, number_of_workers):
    v_time = amount_of_job_done * virtual_time_rate(number_of_workers)
    logger.debug('virtual time %s', v_time)
    return v_time


def enqueue(task, v_current):
    queue = queues[task['producer']]
    v_start = max(v_current, queue['last_v_finish'])
    # divide a size to an allocated service rate
    v_task_length = task['size'] / same_weight_per_queue
    task['v_start'] = v_start
    # v_finish is the time when
    # "the last bit of a packet would have been served in fluid fair queuing simulation"
    # WFQ serves packets in the increasing order of virtual finish times
    # FFQ simulation is the bit by bit round robin
    task['v_finish'] = v_start + v_task_length
    queue['last_v_finish'] = task['v_finish']
    queue['tasks'].append(task)


def dequeue(worker_index, workers_in_total, v_current):
    min_v_finish = inf
    selected_queue = None
    # TODO: in live scheduling, recalculate here finish times according to the correct vt rate?

    # WFQ
    # active = active_queues()

    # WF2Q
    # active = [q for q in active_queues() if q['tasks'][0]['v_start'] <= v_current]

    # 2DFQ
    eligibility = {}
    for queue in active_queues():
        for item in queue['tasks']:
            eligibility[item['name']] = item['v_start'] - worker_index * item['size'] / workers_in_total
    active = [q for q in active_queues() if eligibility[q['tasks'][0]['name']] <= v_current]
    # logger.info('eligibility %s', eligibility)
    # logger.info('active eligible %s', active)

    for queue in active:
        candidate = queue['tasks'][0]
        if candidate['v_finish'] < min_v_finish:
            min_v_finish = candidate['v_finish']
            selected_queue = queue
    # TODO: pop only after finishing the job
    logger.info('current %s', v_current)
    return selected_queue['tasks'].pop(0) if selected_queue is not None else None


def do(task):
    sleep(task['size'])


def run_worker(worker_index, workers_in_total):
    # TODO: v_current is also used for enqueuing, how to get it when in live scheduling?
    v_current = 0
    while True:
        job = dequeue(worker_index, workers_in_total, v_current)
        if job is None:
            # when all queues are empty, i.e. when busy period ends, virtual time goes to zero
            # TODO: should I also reset `last_v_finish` of a queue to zero?
            v_current = 0
            break  # exit while debugging prototype, in live scheduling just keep iterating
        else:
            logger.info('selected %s of size %s', job['name'], job['size'])
            do(job)
            logger.debug('finished %s', job['name'])
            # TODO: in live scheduling get actual number of workers
            v_current += virtual_time(job['size'], workers_in_total)


def produce(number_of_workers):
    tasks = [
        {'producer': 'A', 'size': 1, 'name': 'a1'},
        {'producer': 'A', 'size': 1, 'name': 'a2'},
        {'producer': 'A', 'size': 1, 'name': 'a3'},
        {'producer': 'A', 'size': 1, 'name': 'a4'},
        {'producer': 'A', 'size': 1, 'name': 'a5'},
        {'producer': 'A', 'size': 1, 'name': 'a6'},
        {'producer': 'A', 'size': 1, 'name': 'a7'},
        {'producer': 'A', 'size': 1, 'name': 'a8'},

        {'producer': 'A', 'size': 1, 'name': 'a9'},
        {'producer': 'A', 'size': 1, 'name': 'a10'},
        {'producer': 'A', 'size': 1, 'name': 'a11'},
        {'producer': 'A', 'size': 1, 'name': 'a12'},

        {'producer': 'B', 'size': 1, 'name': 'b1'},
        {'producer': 'B', 'size': 1, 'name': 'b2'},
        {'producer': 'B', 'size': 1, 'name': 'b3'},
        {'producer': 'B', 'size': 1, 'name': 'b4'},
        {'producer': 'B', 'size': 1, 'name': 'b5'},
        {'producer': 'B', 'size': 1, 'name': 'b6'},
        {'producer': 'B', 'size': 1, 'name': 'b7'},
        {'producer': 'B', 'size': 1, 'name': 'b8'},

        {'producer': 'B', 'size': 1, 'name': 'b9'},
        {'producer': 'B', 'size': 1, 'name': 'b10'},
        {'producer': 'B', 'size': 1, 'name': 'b11'},
        {'producer': 'B', 'size': 1, 'name': 'b12'},

        {'producer': 'C', 'size': 10, 'name': 'c1'},
        {'producer': 'C', 'size': 10, 'name': 'c2'},
        {'producer': 'C', 'size': 10, 'name': 'c3'},

        {'producer': 'D', 'size': 10, 'name': 'd1'},
        {'producer': 'D', 'size': 10, 'name': 'd2'},
        {'producer': 'D', 'size': 10, 'name': 'd3'},
    ]

    v_current = virtual_time(0, number_of_workers)
    for task in tasks:
        enqueue(task, v_current)

    for queue in queues.values():
        logger.info([[task['name'], task['v_start'], task['v_finish']] for task in queue['tasks']])


def consume(number_of_workers):
    worker_threads = []
    for worker_index in range(number_of_workers):
        worker_thread = threading.Thread(
            target=run_worker,
            name=str(worker_index),
            args=(worker_index, number_of_workers),
        )
        worker_threads.append(worker_thread)
        worker_thread.start()

    for worker_thread in worker_threads:
        worker_thread.join()


if __name__ == '__main__':
    number_of_workers = 2
    produce(number_of_workers)
    consume(number_of_workers)
    logger.info('Done')
