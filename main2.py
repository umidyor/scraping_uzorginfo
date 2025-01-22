# import asyncio
# import aiohttp
# import pandas as pd
# from bs4 import BeautifulSoup
# from bot import *
# proxy_username = 'umidyor007@gmail.com'
# proxy_password = 'himb9w2aqowp'
# api_key = "be8af53d49bce3a4c518d3b6805b16d2001a1c5477788dd9db1610e2b9a1d03a"
#
# # Proxy URL with authentication
# proxy_url = f"http://{proxy_username}:{proxy_password}@zproxy.lum-superproxy.io:22225"
#
# # Headers (optional, can help avoid detection)
# headers = {
#     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
# }
#
# base_url = "https://uzorg.info/oz/info-id-{i}"  # Page URL pattern
#
# # Semaphore to limit concurrency
# semaphore = asyncio.Semaphore(5)
#
# async def fetch_url(session, url):
#     """Fetch a URL asynchronously with rate limiting."""
#     async with semaphore:
#         try:
#             async with session.get(url, params={'url': proxy_url, 'api_key': api_key}, headers=headers) as response:
#                 await asyncio.sleep(1)  # Rate limit to avoid overloading
#                 response.raise_for_status()
#                 html_content = await response.text()
#                 print(f"Fetched data from {url}")
#                 await problems(f"Fetched data from {url}")
#                 return url, html_content
#         except Exception as e:
#             print(f"Failed to fetch {url}: {e}")
#             await problems(f"Failed to fetch {url}: {e}")
#             return url, None
#
# def extract_headers_and_data(html_content):
#     """Extract headers and data from HTML content."""
#     soup = BeautifulSoup(html_content, "html.parser")
#
#     # Extract columns for headers
#     columns = soup.find_all("div", class_="col-sm-3 text-dark")
#     headers = [column.text.strip() for column in columns]
#
#     # Extract data for each row
#     div = soup.find("div", class_="container pt-3 pb-5")
#     rows = div.find_all("div", class_="row pt-3")
#
#     data = {}
#     for i, row in enumerate(rows):
#         label = row.find("div", class_="col-sm-3 text-dark")
#         value = row.find("div", class_="col-sm-9")
#
#         if label and value:
#             label_text = label.get_text(strip=True)
#             value_text = value.get_text(strip=True)
#
#             if i < len(headers):  # Map values to headers
#                 data[headers[i]] = value_text
#     return headers, data
#
# async def scrape_urls(url_list, output_file):
#     """Scrape data from a list of URLs concurrently and save to CSV immediately."""
#     all_data = []  # List to store all data rows
#
#     async with aiohttp.ClientSession() as session:
#         tasks = [fetch_url(session, url) for url in url_list]
#         results = await asyncio.gather(*tasks)
#         print(results)
#
#         for url, html_content in results:
#             if html_content:
#                 dynamic_headers, data = extract_headers_and_data(html_content)
#                 all_data.append(data)
#
#                 # Immediately save data to CSV after each page is processed
#                 df = pd.DataFrame(all_data)
#                 df.to_csv(output_file, index=False, encoding='utf-8')
#                 print(f"2)Data for {url} saved.")
#                 await problems(f"Data for {url} saved.")
#             else:
#                 print(f"Failed to fetch {url}.")
#
# # Generate URLs for scraping (based on page IDs)
# # url_list = [base_url.format(i=i) for i in range(302845,605690)]
# url_list = [base_url.format(i=i) for i in range(2,50)]
# print(url_list)
#
# output_file = "scrapedtwo_data.csv"
# asyncio.run(scrape_urls(url_list, output_file))
# asyncio.run(filesend("File is ready","scrapedtwo_data.csv"))

import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from bot import *

proxy_username = 'umidyor007@gmail.com'
proxy_password = 'himb9w2aqowp'

# Proxy URL (with embedded authentication credentials)
proxy_url = f"http://{proxy_username}:{proxy_password}@zproxy.lum-superproxy.io:22225"

# Your Bright Data API key
api_key = "be8af53d49bce3a4c518d3b6805b16d2001a1c5477788dd9db1610e2b9a1d03a"

# Headers (optional, but can help avoid detection)
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

base_url = "https://uzorg.info/oz/info-id-{i}"  # Main page URL pattern

# Create a semaphore to limit concurrent requests
semaphore = asyncio.Semaphore(5)  # Limits to 5 concurrent requests


async def fetch_url(session, url):
    """Fetch a URL asynchronously with rate limiting."""
    async with semaphore:
        try:
            async with session.get(url, params={'url': proxy_url, 'api_key': api_key}, headers=headers) as response:
                # Debugging: Check response status and headers
                print(f"Response status for {url}: {response.status}")
                print(f"Response headers: {response.headers}")

                if response.status != 200:
                    print(f"Failed to fetch {url}. Status code: {response.status}")
                    await problems(f"Failed to fetch {url}. Status code: {response.status}")
                    return url, None

                # Check content type to ensure it's HTML
                content_type = response.headers.get('Content-Type', '')
                if 'text/html' not in content_type:
                    print(f"Non-HTML response received from {url}. Content-Type: {content_type}")
                    await problems(f"Non-HTML response received from {url}. Content-Type: {content_type}")
                    return url, None

                # Fetch the HTML content
                html_content = await response.text()

                # Debugging: Print a snippet of the HTML content to inspect it
                print(f"Fetched data from {url}:\n{html_content[:500]}...")  # First 500 chars of the response
                await problems(f"Fetched data from {url}")

                return url, html_content
        except Exception as e:
            print(f"Failed to fetch {url}: {e}")
            await problems(f"Failed to fetch {url}: {e}")
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


async def scrape_urls(url_list, output_file):
    """Scrape data from a list of URLs concurrently and save to CSV immediately."""
    all_data = []  # List to store all data rows

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_url(session, url) for url in url_list]
        results = await asyncio.gather(*tasks)
        print(results)

        for url, html_content in results:
            if html_content:
                dynamic_headers, data = extract_headers_and_data(html_content)
                all_data.append(data)

                # Immediately save data to CSV after each page is processed
                df = pd.DataFrame(all_data)
                df.to_csv(output_file, index=False, encoding='utf-8')
                print(f"Data for {url} saved.")
                await problems(f"Data for {url} saved.")
            else:
                print(f"Failed to fetch {url}.")


# Generate URLs for scraping (based on page IDs)
url_list = [base_url.format(i=i) for i in range(2, 50)]  # Example range of page IDs
print(url_list)

output_file = "scraped_data.csv"
asyncio.run(scrape_urls(url_list, output_file))
asyncio.run(filesend("File is ready", "scraped_data.csv"))
