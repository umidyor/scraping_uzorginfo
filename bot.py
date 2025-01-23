import aiohttp
import random
import asyncio
import os
import pandas as pd
from aiogram import Bot, Dispatcher
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils.exceptions import RetryAfter
from bs4 import BeautifulSoup

ADMINS = 5149506457
bot = Bot(token="5752135237:AAElZDaC1gjKdRloJZXBjxqVisc4xqFg_OQ")
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# Load proxies from file
def load_proxies(file_path):
    with open(file_path, 'r') as f:
        proxies = [line.strip() for line in f if line.strip()]
    return proxies

# Load the proxies into a list
proxy_list = load_proxies("proxyscrape_premium_http_proxies.txt")

# Helper function to send notifications
async def problems(message: str):
    await bot.send_message(chat_id=ADMINS, text=message)

# Function to send a file to the admin
async def filesend(file_path: str):
    try:
        with open(file_path, 'rb') as file:
            await bot.send_document(ADMINS, document=file)
    except Exception as e:
        await problems(f"Problem for filesend: {e}")

# Function to fetch a URL with proxy rotation
async def fetch_url(session, url):
    """Fetch a URL asynchronously with proxy rotation, rate limiting, and retries."""
    retries = 3  # Retry limit for failed requests
    attempt = 0
    while attempt < retries:
        proxy = random.choice(proxy_list)  # Randomly choose a proxy
        proxy_url = f"http://{proxy}"  # Format the proxy correctly
        try:
            async with session.get(url, proxy=proxy_url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                await asyncio.sleep(1)  # Rate limiting to avoid overwhelming the server

                if response.status == 200:
                    html_content = await response.text()
                    print(f"Fetched data from {url} using {proxy}")
                    return url, html_content

                elif response.status == 404:
                    print(f"404 Error: Page {url} not found. Skipping.")
                    return url, None  # Skip URLs that return 404

                else:
                    print(f"Unexpected status {response.status} for {url}. Retrying...")

        except aiohttp.ClientResponseError as e:
            print(f"ClientError: Failed to fetch {url}, attempt {attempt + 1}/{retries}: {e}")

        except aiohttp.ClientConnectionError as e:
            print(f"ConnectionError: Failed to connect to {url}, attempt {attempt + 1}/{retries}: {e}")

        except asyncio.TimeoutError:
            print(f"TimeoutError: Request to {url} timed out, attempt {attempt + 1}/{retries}")

        except RetryAfter as e:
            print(f"Flood control exceeded. Sleeping for {e.timeout} seconds.")
            await asyncio.sleep(e.timeout)  # Wait and retry

        except Exception as e:
            print(f"Unexpected error: Failed to fetch {url}, attempt {attempt + 1}/{retries}: {e}")

        attempt += 1
        await asyncio.sleep(2)

    return url, None

# Extract headers and data from HTML content
def extract_headers_and_data(html_content):
    """Extract headers and data from HTML content."""
    soup = BeautifulSoup(html_content, "html.parser")

    columns = soup.find_all("div", class_="col-sm-3 text-dark")
    headers = [column.text.strip() for column in columns]

    div = soup.find("div", class_="container pt-3 pb-5")
    rows = div.find_all("div", class_="row pt-3")

    data = {}
    for i, row in enumerate(rows):
        label = row.find("div", class_="col-sm-3 text-dark")
        value = row.find("div", class_="col-sm-9")

        if label and value:
            label_text = label.get_text(strip=True)
            value_text = value.get_text(strip=True)

            if i < len(headers):
                data[headers[i]] = value_text
    return headers, data

# Scrape data from the list of URLs
async def scrape_urls(url_list):
    """Scrape data from a list of URLs concurrently and save to CSV in batches."""
    batch_size = 100
    semaphore = asyncio.Semaphore(10)  # Limit concurrent requests

    async with aiohttp.ClientSession() as session:
        for batch_number, i in enumerate(range(0, len(url_list), batch_size), start=1):
            batch_urls = url_list[i:i + batch_size]
            print(f"Processing batch {batch_number} with {len(batch_urls)} URLs...")
            await problems(f"Processing batch {batch_number} with {len(batch_urls)} URLs...")

            async def limited_fetch(url):
                async with semaphore:
                    return await fetch_url(session, url)

            tasks = [limited_fetch(url) for url in batch_urls]
            results = await asyncio.gather(*tasks)

            all_data = []
            for url, html_content in results:
                if html_content:
                    dynamic_headers, data = extract_headers_and_data(html_content)
                    all_data.append(data)
                else:
                    print(f"Skipping {url} due to fetch failure.")

            if all_data:
                output_file = f"CSV/scraped_{batch_number * batch_size}.csv"
                df = pd.DataFrame(all_data)
                with open(output_file, 'a', newline='', encoding='utf-8') as f:
                    df.to_csv(f, index=False, header=not f.tell(), mode='a')
                    f.flush()
                    os.fsync(f.fileno())
                print(f"Batch {batch_number} saved to {output_file}.", flush=True)
                await problems(f"Batch {batch_number} saved to {output_file}.")
                await filesend(output_file)

            print(f"Sleeping for 1 minute after batch {batch_number}...")
            await problems(f"Sleeping for 1 minute after batch {batch_number}...")
            await asyncio.sleep(60)

# List of URLs to scrape
base_url = "https://uzorg.info/oz/info-id-{i}"
url_list = [base_url.format(i=i) for i in range(300, 1514225)]
print(f"Total URLs to scrape: {len(url_list)}")

# Start scraping
asyncio.run(scrape_urls(url_list))
