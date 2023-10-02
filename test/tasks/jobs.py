from config.celery import app
import time


@app.task
def job1():
    time.sleep(1)
    return "Task 1"


@app.task
def job2():
    time.sleep(2)
    return "Task 2"


@app.task
def job3():
    time.sleep(3)
    return "Task 3"


@app.task
def job4():
    time.sleep(1)
    return "Task 4"


@app.task
def job5():
    time.sleep(2)
    return "Task 5"
