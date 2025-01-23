import aiohttp
import random
import asyncio
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}


# Load proxies from file
def load_proxies(file_path):
    with open(file_path, 'r') as f:
        proxies = [line.strip() for line in f if line.strip()]
    return proxies

# Load the proxies into a list
proxy_list = load_proxies("proxyscrape_premium_http_proxies.txt")

async def fetch_url(url):
    proxy = random.choice(proxy_list)  # Randomly choose a proxy
    proxy_url = f"http://{proxy}"  # Format the proxy correctly

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, proxy=proxy_url, timeout=30) as response:
                if response.status == 200:
                    html_content = await response.text()
                    print(f"Fetched data from {url} using {proxy}")
                    return html_content
                else:
                    print(f"Failed to fetch {url}, status: {response.status}")
        except Exception as e:
            print(f"Error fetching {url} with proxy {proxy}: {e}")
        return None

async def fetch_with_rotation(url):
    while proxy_list:
        proxy = random.choice(proxy_list)
        proxy_url = f"http://{proxy}"

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, proxy=proxy_url, timeout=30) as response:
                    if response.status == 200:
                        print(f"Successfully fetched {url} using {proxy}")
                        return await response.text()
            except Exception as e:
                print(f"Proxy {proxy} failed, removing it. Error: {e}")
                proxy_list.remove(proxy)  # Remove failing proxy
                if not proxy_list:
                    print("No more working proxies left.")
                    return None
            await asyncio.sleep(random.uniform(1, 3))  # Random delay to mimic human behavior

async def validate_proxy(proxy):
    test_url = "https://uzorg.info/oz/info-id-17000"  # Test proxy by checking public IP
    proxy_url = f"http://{proxy}"

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(test_url, proxy=proxy_url, timeout=10) as response:
                if response.status == 200:
                    print(proxy)
                    return True
        except Exception:
            print(f"Proxy {proxy} failed")
    return False

# Validate all proxies
async def validate_proxies(proxy_list):
    valid_proxies = []
    for proxy in proxy_list:
        if await validate_proxy(proxy):
            valid_proxies.append(proxy)
    return valid_proxies

valid_proxies = asyncio.run(validate_proxies(proxy_list))
print(f"Valid proxies: {valid_proxies}")

