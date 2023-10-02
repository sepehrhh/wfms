from celery import Celery


app = Celery('workflow',
             backend='redis://localhost:6380/4',
             broker='pyamqp://guest@localhost:5673//',
             include=['tasks.jobs', 'tasks.warmups'])

app.conf.task_routes = {
    'tasks.warmups.*': {
        'priority': 5,
    },
    'tasks.jobs.*': {
        'priority': 10,
    }
}

if __name__ == '__main__':
    app.start()
