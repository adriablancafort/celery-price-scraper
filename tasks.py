from main import app
from database import get_database
from proxies import get_proxies, ProxyRotator
from retailers.amazon import get_amazon_price
from retailers.tradeinn import get_tradeinn_prices
from retailers.pccomponentes import get_pccomponentes_prices


db = get_database()
proxies = get_proxies()
proxy_rotator = ProxyRotator(proxies)


@app.task
def process_product(product):
    """Check product price in retailer website and store it in the database."""

    url = str(product["url"])
    if "amazon" in url: get_amazon_price(db, proxy_rotator, product)
    elif "tradeinn" in url: get_tradeinn_prices(db, proxy_rotator, product)
    elif "pccomponentes" in url: get_pccomponentes_prices(db, proxy_rotator, product)
    else: print(f"Unknown retailer: {url}")
