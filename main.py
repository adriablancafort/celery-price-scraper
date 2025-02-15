from dotenv import load_dotenv
from os import getenv
from celery import Celery
from celery.schedules import crontab
from database import get_database, iterate_collection
from proxies import get_proxies, ProxyRotator
from retailers.amazon import get_amazon_price
from retailers.tradeinn import get_tradeinn_prices
from retailers.pccomponentes import get_pccomponentes_prices


load_dotenv()

rabbit_ip = getenv("RABBITMQ_IP")
rabbit_port = getenv("RABBITMQ_PORT")
rabbit_user = getenv("RABBITMQ_USER")
rabbit_password = getenv("RABBITMQ_PASSWORD")

broker_url = f"amqp://{rabbit_user}:{rabbit_password}@{rabbit_ip}:{rabbit_port}//"

app = Celery("tasks", broker=broker_url)

db = get_database()
proxies = get_proxies()
proxy_rotator = ProxyRotator(proxies)


@app.task
def enqueue_products():
    """Fetch products from MongoDB and enqueue them for processing."""
    
    for product in iterate_collection(db, "monitored"):
        task_product = {
            "url": product["url"],
            "variant_id": str(product["variant_id"]), # Convert to string to avoid JSON serialization issues
        }
        process_product.delay(task_product)


@app.task
def process_product(product):
    """Check product price in retailer website and store it in the database."""

    url = str(product["url"])
    if "amazon" in url: get_amazon_price(db, proxy_rotator, product)
    elif "tradeinn" in url: get_tradeinn_prices(db, proxy_rotator, product)
    elif "pccomponentes" in url: get_pccomponentes_prices(db, proxy_rotator, product)
    else: print(f"Unknown retailer: {url}")


app.conf.beat_schedule = {
    'check-prices-daily': {
        'task': 'main.enqueue_products',
        'schedule': crontab(minute=0, hour=0),
    },
}