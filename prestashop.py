import requests


def get_access_token(base_url: str, client_id: str, client_secret: str) -> str:
    """Get access token from PrestaShop Admin API."""

    url = f"{base_url}/admin-api/access_token"
    
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials',
        'scope[]': [
            'product_read',
            'product_write'
        ]
    }
    
    try:
        response = requests.post(url, data=data)
        response.raise_for_status()
        print("Access token created")
        return response.json()['access_token']
    except requests.exceptions.RequestException as e:
        print(f"Failed to create access token: {e}")
        return None
    

def update_product_price(base_url: str, access_token: str, product_id: int, price: str) -> None:
    """Update product price using PrestaShop Admin API."""

    url = f"{base_url}/admin-api/product/{product_id}"
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Prefer': 'return=minimal'  # Tells API we don't need response body
    }
    
    data = {
        'price': str(price)
    }
    
    try:
        response = requests.patch(url, headers=headers, json=data)
        response.raise_for_status()
        print(f"updated product_id: {product_id}, price: {price}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to update product price: {e}")