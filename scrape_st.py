import requests
from bs4 import BeautifulSoup
import time

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:106.0) Gecko/20100101 Firefox/106.0",
    # "User-Agent": random.choice(common_user_agents),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    # "Referer": "https://www.realestate.com.au/property/",
    "DNT": "1",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "te": "trailers",
    "upgrade-insecure-requests": "1"
}

with open("urls.txt", 'w') as f:
    count = 0
    for street in [
        # "https://www.realestate.com.au/wa/burswood-6100/twickenham-rd/",
        "https://www.realestate.com.au/wa/como-6152/hobbs-ave/"
    ]:
        res = requests.get(street, headers=headers)
        soup = BeautifulSoup(res.content, 'html.parser')
        print(soup.prettify())
        urls = [ a['href'] for a in soup.find_all("a", "property-card-link") ]
        count = len(urls)
        if not count:
            raise Exception(f"no urls found for {street}")
        else:
            print(f"Found {count} urls for {street}")
        for url in urls:
            f.write(url + "\n")
        time.sleep(5)