from config.celery import app
import time


@app.task
def warmup1():
    time.sleep(2)
    return "Warmup Job 1 Done"


@app.task
def warmup2():
    time.sleep(3)
    return "Warmup Job 2 Done"


@app.task
def warmup3():
    time.sleep(1)
    return "Warmup Job 3 Done"


@app.task
def warmup4():
    time.sleep(4)
    return "Warmup Job 4 Done"


@app.task
def warmup5():
    time.sleep(2)
    return "Warmup Job 5 Done"
