from curl_cffi import requests
import os


class ProxyRotator:
    def __init__(self, proxies: list[str]) -> None:
        self.proxies = proxies
        self.sessions = [requests.Session() for _ in proxies]
        self.current_index = 0

    def _rotate_proxy(self) -> None:
        self.current_index = (self.current_index + 1) % len(self.proxies)

    def _remove_proxy(self) -> None:
        del self.proxies[self.current_index]
        del self.sessions[self.current_index]

    def get_content(self, url: str, headers: dict = None, cookies: dict = None) -> str:
        """Gets the content of the given URL through a proxy."""

        while self.proxies:
            self._rotate_proxy()
            proxy = self.proxies[self.current_index]
            session = self.sessions[self.current_index]
            try:
                response = session.get(url, impersonate="safari", proxy=proxy, timeout=3000, headers=headers, cookies=cookies)
                if response.status_code == 200:
                    return response.text
                else:
                    print(f"{url} {response.status_code}")
            except Exception as error:
                print(f"{url} {error}")
                self._remove_proxy()
        raise Exception("No proxies left")


def get_proxies() -> list[str]:
    """Returns a list of proxies."""

    proxies = []

    file_path = os.path.join(os.path.dirname(__file__), "proxies.txt")
    with open(file_path, "r") as file:
        for line in file:
            ip, port, username, password = line.strip().split(":")
            proxy = f"http://{username}:{password}@{ip}:{port}"
            proxies.append(proxy)

    return proxies