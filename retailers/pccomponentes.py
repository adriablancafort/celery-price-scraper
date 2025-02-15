from database import Database, store_price
from bson.objectid import ObjectId
from proxies import ProxyRotator
import json


PCCOMPONENTES_ID = "679a221c21cc010007e27941"


def get_pccomponentes_prices(db: Database, proxy_rotator: ProxyRotator, product: dict) -> None:
    """Gets the prices of the given product in all available languages and stores it in the database."""

    product["variant_id"] = ObjectId(product["variant_id"])
    product["retailer_id"] = ObjectId(PCCOMPONENTES_ID)

    region_id = "67942b3721cc010007e278df" # España
    product["region_id"] = ObjectId(region_id) 
    
    url =  product["url"]

    product_id = "10855022"
    api_url = f"https://www.pccomponentes.com/api/articles/{product_id}/buybox"

    response = proxy_rotator.get_content(api_url)
    product_json = json.loads(response)

    price = product_json["buyBox"][0]["totalPrice"]

    store_price(db, product, price)
    print(f"url: {url}, region_id: {region_id}, price: {price}")