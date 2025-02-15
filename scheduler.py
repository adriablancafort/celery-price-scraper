from celery.schedules import crontab
from main import app
from database import get_database, iterate_collection


db = get_database()


@app.task
def enqueue_urls():
    """Fetch URLs from MongoDB and enqueue them for processing."""
    
    for product in iterate_collection("monitored"):
        app.send_task("tasks.process_product", args=[product])


app.conf.beat_schedule = {
    'check-prices-hourly': {
        'task': 'scheduler.enqueue_urls',
        'schedule': crontab(minute=0),
    },
}
