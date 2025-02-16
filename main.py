from dotenv import load_dotenv
from os import getenv
from celery import Celery, group
from celery.schedules import crontab
from database import get_database, yield_products, yield_prices
from proxies import get_proxies, ProxyRotator
from prestashop import get_access_token
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

mongo_ip = getenv("MONGO_IP")
mongo_port = int(getenv("MONGO_PORT"))
mongo_user = getenv("MONGO_USER")
mongo_password = getenv("MONGO_PASSWORD")
mongo_db_name = getenv("MONGO_DB_NAME")

mongo_url = f"mongodb://{mongo_user}:{mongo_password}@{mongo_ip}:{mongo_port}"

db = get_database(mongo_url, mongo_db_name)
proxies = get_proxies()
proxy_rotator = ProxyRotator(proxies)


@app.task
def enqueue_products():
    """Fetch products from MongoDB and enqueue them for processing."""
    
    check_tasks = group([
        check_price.s(product)
        for product in yield_products(db)
    ])
    
    # Chain the checks with enqueue_prices
    (check_tasks | enqueue_prices.si()).apply_async()


@app.task
def check_price(product):
    """Check product price in retailer website and store it in the database."""

    url = str(product["url"])
    if "amazon" in url: get_amazon_price(db, proxy_rotator, product)
    elif "tradeinn" in url: get_tradeinn_prices(db, proxy_rotator, product)
    elif "pccomponentes" in url: get_pccomponentes_prices(db, proxy_rotator, product)
    else: print(f"Unknown retailer: {url}")


@app.task
def enqueue_prices():
    """Fetch prices from MongoDB and enqueue them for processing."""

    base_url = getenv('PRESTASHOP_BASE_URL')
    client_id = getenv('PRESTASHOP_CLIENT_ID')
    client_secret = getenv('PRESTASHOP_CLIENT_SECRET')

    access_token = get_access_token(base_url, client_id, client_secret)
    
    process_tasks = group([
        process_price.s(base_url, access_token, prices)
        for prices in yield_prices(db)
    ])

    process_tasks.apply_async()


@app.task
def process_price(prices, base_url, access_token):
    """Compute optimal price, store it in the database and update it on the website."""

    process_price(db, base_url, access_token, prices)


app.conf.beat_schedule = {
    'run-daily': {
        'task': 'main.enqueue_products',
        'schedule': crontab(minute=40, hour=19),
    }
}
