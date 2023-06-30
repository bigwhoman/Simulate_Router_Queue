import random
from queue import PriorityQueue
import numpy as np
import matplotlib.pyplot as plt

SIMULATION_RUNNING_TIME = 10000
SERVICE_TIME_LAMBDA = 5
GENERATOR_TIME_LAMBDA = 5
MAX_QUEUE_SIZE = 10
PROCESSOR_COUNT = 4

class Task:
    def __init__(self, id: int, arrival_time: int):
        self.id = id
        self.service_time = int(random.expovariate(1 / SERVICE_TIME_LAMBDA))
        self.priority = self.generate_priority()
        self.arrival_time = arrival_time
        self.start_service_time = None
        self.finish_time = None
        
    def generate_priority(self):
        r = random.random()
        if r <= 0.2:
            return 'high'
        elif r <= 0.5:
            return 'medium'
        else:
            return 'low'

    def __lt__(self, other):
        return self.service_time < other.service_time

def task_generator() -> list[Task]:
    current_time = 0
    task_id = 0
    tasks: list[Task] = []
    while current_time < SIMULATION_RUNNING_TIME:
        task_id += 1
        inter_arrival_time = int(random.expovariate(1 / GENERATOR_TIME_LAMBDA))
        current_time += inter_arrival_time
        tasks.append(Task(task_id, current_time))
    return tasks

def fifo_policy(queue):
    try:
        return queue.get_nowait()
    except:
        return None

def weighted_round_robin_policy(queue):
    try:
        return queue["high"].get_nowait()
    except:
        pass
    try:
        return queue["medium"].get_nowait()
    except:
        pass
    try:
        return queue["low"].get_nowait()
    except:
        pass
    return None

def non_preemptive_priority_policy(queue):
    sorted_tasks = sorted(queue.queue, key=lambda task: task.priority)
    if len(sorted_tasks) == 0:
        return None
    task = sorted_tasks.pop(0)
    queue.queue.remove(task)
    return task

def run_simulation(policy):
    if policy == 'weighted_round_robin':
        task_queues = [{'high': PriorityQueue(maxsize=MAX_QUEUE_SIZE), 
                        'medium': PriorityQueue(maxsize=MAX_QUEUE_SIZE), 
                        'low': PriorityQueue(maxsize=MAX_QUEUE_SIZE)} for _ in range(PROCESSOR_COUNT)]
    else:
        task_queues = [PriorityQueue(maxsize=MAX_QUEUE_SIZE) for _ in range(PROCESSOR_COUNT)]
    dropped_tasks = 0
    running_task = [None] * PROCESSOR_COUNT
    total_queue_sizes = [0] * PROCESSOR_COUNT
    total_task_times = [0] * PROCESSOR_COUNT
    server_busy_times = [0] * PROCESSOR_COUNT
    tasks = task_generator()
    current_time = 0
    while current_time < SIMULATION_RUNNING_TIME or any(map(lambda x: x != None, running_task)):
        # Check each core for done tasks
        for core in range(PROCESSOR_COUNT):
            if running_task[core] != None: # if core is running something...
                if running_task[core].finish_time <= current_time: # and it's done
                    running_task[core] = None # remove it
        # Enqueue stuff in queue
        for task in tasks:
            if task.arrival_time == current_time:
                if policy == 'weighted_round_robin':
                    min_queue = min(task_queues, key=lambda q: q[task.priority].qsize())
                    if min_queue[task.priority].full():
                        dropped_tasks += 1
                    else:
                        min_queue_idx = task_queues.index(min_queue)
                        total_queue_sizes[min_queue_idx] += 1
                        total_task_times[min_queue_idx] += task.service_time
                        server_busy_times[min_queue_idx] += task.service_time
                        min_queue[task.priority].put(task)
                else:
                    min_queue = min(task_queues, key=lambda q: q.qsize())
                    if min_queue.full():
                        dropped_tasks += 1
                    else:
                        min_queue_idx = task_queues.index(min_queue)
                        total_queue_sizes[min_queue_idx] += 1
                        total_task_times[min_queue_idx] += task.service_time
                        server_busy_times[min_queue_idx] += task.service_time
                        min_queue.put(task)
        # Execute new task
        for core in range(PROCESSOR_COUNT):
            if running_task[core] == None: # if core is running nothing...
                if policy == 'fifo':
                    task = fifo_policy(task_queues[core])
                elif policy == 'weighted_round_robin':
                    task = weighted_round_robin_policy(task_queues[core])
                elif policy == 'non_preemptive_priority':
                    task = non_preemptive_priority_policy(task_queues[core])
                else:
                    raise ValueError(f"Unknown policy: {policy}")
                if task:
                    task.start_service_time = current_time
                    task.finish_time = current_time + task.service_time
                    running_task[core] = task
        current_time += 1

    cook = []
    for task in tasks:
        if task.priority == 'high':
            if task.finish_time != None:
                running_time = task.finish_time - task.arrival_time
                cook.append(running_time)
    avg_queue_sizes = [size / len(tasks) for size in total_queue_sizes]
    avg_time_spent_queues = [task_time / len(tasks) for task_time in total_task_times]
    avg_server_utilization = [busy_time / current_time for busy_time in server_busy_times]
    return tasks, dropped_tasks, avg_queue_sizes, avg_time_spent_queues, avg_server_utilization, cook


scheduling_policies = ['fifo', 'weighted_round_robin', 'non_preemptive_priority']
for policy in scheduling_policies:
    tasks, dropped_tasks, avg_queue_sizes, avg_time_spent_queues, avg_server_utilization, high_priority_times = run_simulation(policy)
    print(f"Scheduling with {policy} policy")
    print(f"Total tasks generated: {len(tasks)}")
    print(f"Dropped tasks: {dropped_tasks}")
    print(f"Average queue sizes: {avg_queue_sizes}")
    print(f"Average time spent in all queues: {sum(avg_time_spent_queues) / len(avg_time_spent_queues)}")
    print(f"Average time spent in each queue: {avg_time_spent_queues}")
    print(f"Average server utilization: {avg_server_utilization}")
    print("===================================")

scheduling_policies = ['fifo', 'weighted_round_robin', 'non_preemptive_priority']
high_priority_times_by_policy = {}
for policy in scheduling_policies:
    dropped_tasks, total_tasks, avg_queue_sizes, avg_time_spent_queues, avg_server_utilization,high_priority_times = run_simulation(policy)
    high_priority_times_by_policy[policy] = high_priority_times

for policy, high_priority_times in high_priority_times_by_policy.items():
    sorted_times = np.sort(high_priority_times)
    yvals = np.arange(1, len(sorted_times) + 1) / len(sorted_times)
    plt.plot(sorted_times, yvals, label=policy)

plt.xlabel('Time spent in queues')
plt.ylabel('CDF')
plt.legend(loc='upper left')
plt.title('CDF of time spent in queues for high-priority tasks - * 10^-3')
plt.show()