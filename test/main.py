from tasks import jobs, warmups
from celery import group, chord, chain
import time


def workflow():
    concurrent_tasks = group(chain(warmups.warmup1.si(), jobs.job1.si()),
                             chain(warmups.warmup2.si(), jobs.job2.si()),
                             chain(warmups.warmup3.si(), jobs.job3.si()),
                             chain(warmups.warmup4.si(), warmups.warmup5.si()))

    sequential_tasks = chain(chain(jobs.job4.si(), jobs.job5.si()))
    workflow = chord(concurrent_tasks)(sequential_tasks)
    workflow.get()
    return workflow.result


start_time = time.time()
result = workflow()
end_time = time.time()
execution_time = end_time - start_time

print(f"Execution Time: {execution_time} seconds")
print("-------------------------------")
