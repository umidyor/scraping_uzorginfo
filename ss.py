import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup


api_key = "08805ae8991561e7b31e1fc421e53939"
base_url = "https://uzorg.info/oz/info-id-{i}"  # Main page URL pattern

# Create a semaphore to limit concurrent requests
semaphore = asyncio.Semaphore(5)  # Limits to 5 concurrent requests

# Function to fetch URL content asynchronously with rate limiting
import aiohttp
import asyncio
from aiogram.utils.exceptions import RetryAfter


async def fetch_url(session, url):
    """Fetch a URL asynchronously with rate limiting and retry on failure."""
    retries = 3  # Retry limit for failed requests
    attempt = 0
    while attempt < retries:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                await asyncio.sleep(1)  # Rate limiting to avoid overwhelming the server

                if response.status == 200:
                    html_content = await response.text()
                    print(f"Fetched data from {url}")
                    # await problems(f"Fetched data from {url}")
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


def extract_headers_and_data(html_content):
    """Extract headers and data from HTML content."""
    soup = BeautifulSoup(html_content, "html.parser")

    # Extract columns for headers
    columns = soup.find_all("div", class_="col-sm-3 text-dark")
    headers = [column.text.strip() for column in columns]

    # Extract data for each row
    div = soup.find("div", class_="container pt-3 pb-5")
    rows = div.find_all("div", class_="row pt-3")

    data = {}
    for i, row in enumerate(rows):
        label = row.find("div", class_="col-sm-3 text-dark")
        value = row.find("div", class_="col-sm-9")

        if label and value:
            label_text = label.get_text(strip=True)
            value_text = value.get_text(strip=True)

            if i < len(headers):  # Map values to headers
                data[headers[i]] = value_text
    return headers, data
import os

async def scrape_urls(url_list):
    """Scrape data from a list of URLs concurrently and save to CSV in batches."""
    batch_size = 5
    semaphore = asyncio.Semaphore(10)  # Limit concurrent requests

    async with aiohttp.ClientSession() as session:
        for batch_number, i in enumerate(range(0, len(url_list), batch_size), start=1):
            batch_urls = url_list[i:i + batch_size]
            print(f"Processing batch {batch_number} with {len(batch_urls)} URLs...")

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
                output_file = f"scraped_{batch_number * batch_size}.csv"
                df = pd.DataFrame(all_data)
                with open(output_file, 'a', newline='', encoding='utf-8') as f:
                    df.to_csv(f, index=False, header=not f.tell(), mode='a')
                    f.flush()
                    os.fsync(f.fileno())
                print(f"Batch {batch_number} saved to {output_file}.", flush=True)
                await filesend("File is ready!", output_file)

            print(f"Sleeping for 1 minute after batch {batch_number}...")
            await asyncio.sleep(60)


url_list = [base_url.format(i=i) for i in range(2, 1514225)]

print(f"Total URLs to scrape: {len(url_list)}")

# Run the scraper
# asyncio.run(scrape_urls(url_list))
asyncio.run(filesend("afafa","scraped_5.csv"))
