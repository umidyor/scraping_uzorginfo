# import requests,asyncio
# from bs4 import BeautifulSoup
#
#
# # api_key = "400ca7a2f1867dfd87570a6053393443"
# # url = "https://uzorg.info/oz/companies"
# # page = requests.get(f"https://api.scraperapi.com?api_key={api_key}&url={url}")
# # soup=BeautifulSoup(page.content,"html.parser")
# # pages=soup.find("div", class_="col text-center")
# # print(pages)
# # l=[]
# # everylist=soup.find_all("a",class_="list-group-item list-group-item-action flex-column align-items-start")
# # for main_pages in everylist:
# #     l.append(main_pages.get('href'))
# lang=['https://uzorg.info/oz/info-id-2', 'https://uzorg.info/oz/info-id-3', 'https://uzorg.info/oz/info-id-4', 'https://uzorg.info/oz/info-id-5', 'https://uzorg.info/oz/info-id-6', 'https://uzorg.info/oz/info-id-7', 'https://uzorg.info/oz/info-id-8', 'https://uzorg.info/oz/info-id-9', 'https://uzorg.info/oz/info-id-10', 'https://uzorg.info/oz/info-id-11', 'https://uzorg.info/oz/info-id-12', 'https://uzorg.info/oz/info-id-13', 'https://uzorg.info/oz/info-id-14', 'https://uzorg.info/oz/info-id-15', 'https://uzorg.info/oz/info-id-16', 'https://uzorg.info/oz/info-id-17', 'https://uzorg.info/oz/info-id-18', 'https://uzorg.info/oz/info-id-19', 'https://uzorg.info/oz/info-id-20', 'https://uzorg.info/oz/info-id-21', 'https://uzorg.info/oz/info-id-22', 'https://uzorg.info/oz/info-id-23', 'https://uzorg.info/oz/info-id-24', 'https://uzorg.info/oz/info-id-25', 'https://uzorg.info/oz/info-id-26', 'https://uzorg.info/oz/info-id-27', 'https://uzorg.info/oz/info-id-28', 'https://uzorg.info/oz/info-id-29', 'https://uzorg.info/oz/info-id-30', 'https://uzorg.info/oz/info-id-31', 'https://uzorg.info/oz/info-id-32', 'https://uzorg.info/oz/info-id-33', 'https://uzorg.info/oz/info-id-34', 'https://uzorg.info/oz/info-id-35', 'https://uzorg.info/oz/info-id-36', 'https://uzorg.info/oz/info-id-37', 'https://uzorg.info/oz/info-id-38', 'https://uzorg.info/oz/info-id-39', 'https://uzorg.info/oz/info-id-40', 'https://uzorg.info/oz/info-id-41', 'https://uzorg.info/oz/info-id-42', 'https://uzorg.info/oz/info-id-43', 'https://uzorg.info/oz/info-id-44', 'https://uzorg.info/oz/info-id-45', 'https://uzorg.info/oz/info-id-46', 'https://uzorg.info/oz/info-id-47', 'https://uzorg.info/oz/info-id-48', 'https://uzorg.info/oz/info-id-49', 'https://uzorg.info/oz/info-id-50', 'https://uzorg.info/oz/info-id-51', 'https://uzorg.info/oz/info-id-52', 'https://uzorg.info/oz/info-id-53', 'https://uzorg.info/oz/info-id-54', 'https://uzorg.info/oz/info-id-55', 'https://uzorg.info/oz/info-id-56', 'https://uzorg.info/oz/info-id-57', 'https://uzorg.info/oz/info-id-58', 'https://uzorg.info/oz/info-id-59', 'https://uzorg.info/oz/info-id-60', 'https://uzorg.info/oz/info-id-61', 'https://uzorg.info/oz/info-id-62', 'https://uzorg.info/oz/info-id-63', 'https://uzorg.info/oz/info-id-64', 'https://uzorg.info/oz/info-id-65', 'https://uzorg.info/oz/info-id-66', 'https://uzorg.info/oz/info-id-67', 'https://uzorg.info/oz/info-id-68', 'https://uzorg.info/oz/info-id-69', 'https://uzorg.info/oz/info-id-70', 'https://uzorg.info/oz/info-id-71', 'https://uzorg.info/oz/info-id-72', 'https://uzorg.info/oz/info-id-73', 'https://uzorg.info/oz/info-id-74', 'https://uzorg.info/oz/info-id-75', 'https://uzorg.info/oz/info-id-76', 'https://uzorg.info/oz/info-id-77', 'https://uzorg.info/oz/info-id-78', 'https://uzorg.info/oz/info-id-79', 'https://uzorg.info/oz/info-id-80', 'https://uzorg.info/oz/info-id-81', 'https://uzorg.info/oz/info-id-82', 'https://uzorg.info/oz/info-id-83', 'https://uzorg.info/oz/info-id-84', 'https://uzorg.info/oz/info-id-85', 'https://uzorg.info/oz/info-id-86', 'https://uzorg.info/oz/info-id-87', 'https://uzorg.info/oz/info-id-88', 'https://uzorg.info/oz/info-id-89', 'https://uzorg.info/oz/info-id-90', 'https://uzorg.info/oz/info-id-91', 'https://uzorg.info/oz/info-id-92', 'https://uzorg.info/oz/info-id-93', 'https://uzorg.info/oz/info-id-94', 'https://uzorg.info/oz/info-id-95', 'https://uzorg.info/oz/info-id-96', 'https://uzorg.info/oz/info-id-97', 'https://uzorg.info/oz/info-id-98', 'https://uzorg.info/oz/info-id-99', 'https://uzorg.info/oz/info-id-100', 'https://uzorg.info/oz/info-id-101']
# # import asyncio
# # import aiohttp
# # from bs4 import BeautifulSoup
# #
# # api_key = "400ca7a2f1867dfd87570a6053393443"
# # url = "https://uzorg.info/oz/companies"
# #
# #
# # async def fetch_url(session, url):
# #     """Fetch a URL asynchronously."""
# #     try:
# #         async with session.get(f"https://api.scraperapi.com?api_key={api_key}&url={url}") as response:
# #             response.raise_for_status()
# #             html_content = await response.text()
# #             return url, html_content
# #     except Exception as e:
# #         print(f"Failed to fetch {url}: {e}")
# #         return url, None
# #
# #
# # async def scrape_urls(url_list):
# #     """Scrape data from a list of URLs concurrently."""
# #     combined_html = ""  # To store all HTML content from all pages
# #
# #     async with aiohttp.ClientSession() as session:
# #         tasks = [fetch_url(session, url) for url in url_list]
# #         results = await asyncio.gather(*tasks)
# #
# #         for url, html_content in results:
# #             if html_content:
# #                 print(f"Fetched data from {url}")
# #                 combined_html += html_content + "\n"  # Append each HTML content
# #
#
#
#
# import re
#
# # Run the script
# # asyncio.run(scrape_urls(lang))
#
# import csv
# from bs4 import BeautifulSoup
# #
# # # Open the HTML file and read its content
# # with open("", "r", encoding="utf-8") as file:
# #     content = file.read()
# #
# # # Parse the HTML with BeautifulSoup
# # soup = BeautifulSoup(content, "html.parser")
# #
# # # Find the rows of data
# # rows = soup.find_all("div", class_="row pt-3")
# #
# # # Prepare a list to hold the headers (columns)
# # headers = []
# # data = []
# #
# # # Loop through each row to extract the header (label) and value (content)
# # for row in rows:
# #     label = row.find("div", class_="col-sm-3 text-dark")  # Label is in the col-sm-3 text-dark div
# #     value = row.find("div", class_="col-sm-9")  # Value is in the col-sm-9 div
# #
# #     if label and value:
# #         # Clean and extract the text, and remove any extra whitespace or newline
# #         label_text = label.get_text(strip=True)
# #         value_text = value.get_text(strip=True)
# #
# #         # Add the label to headers if not already added
# #         if label_text not in headers:
# #             headers.append(label_text)
# #
# #         # Append the value to data (the first entry should be the same for all rows)
# #         data.append(value_text)
# #
# # # Write the cleaned and organized data to a CSV file
# # with open("scraped_data.csv", "w", newline="", encoding="utf-8") as file:
# #     writer = csv.writer(file)
# #     writer.writerow(headers)  # Write the header row
# #     writer.writerow(data)  # Write the data row
# #
# # print("Data has been written to 'scraped_data.csv'")
# #
# #
# # import pandas as pd
# # df=pd.read_csv("scraped_data.csv",sep="|")
# # print(df.head())
#
#
# with open("02-101.html", "r", encoding="utf-8") as file:
#     content = file.read()
#     soup=BeautifulSoup(content,"html.parser")
#     div=soup.find("div",class_="container pt-3 pb-5")
#     row=div.find_all("div",class_="col-sm-9")
#     for i in row:
#         print(i.text)

import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from bot import *
import aiogram
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
                    await problems(f"Fetched data from {url}")
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
        await asyncio.sleep(2)  # Wait before retrying

    return url, None  # After all retries, return None for failed fetch


# Function to extract headers and data from HTML content
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

# Function to scrape URLs and save data to CSV in batches
# async def scrape_urls(url_list):
#     """Scrape data from a list of URLs concurrently and save to CSV in batches."""
#     batch_size = 100
#     all_data = []  # List to store all data rows
#     batch_number = 1  # Start from batch 1
#
#     async with aiohttp.ClientSession() as session:
#         for i in range(0, len(url_list), batch_size):
#             batch_urls = url_list[i:i + batch_size]
#             print(batch_urls)
#             tasks = [fetch_url(session, url) for url in batch_urls]
#             results = await asyncio.gather(*tasks)
#
#             for url, html_content in results:
#                 if html_content:
#                     dynamic_headers, data = extract_headers_and_data(html_content)
#                     all_data.append(data)
#                 else:
#                     print(f"Failed to fetch {url}.")
#
#             # Save data to CSV after every batch
#             output_file = f"scraped_{batch_number * batch_size}.csv"
#             df = pd.DataFrame(all_data)
#             df.to_csv(output_file, index=False, encoding='utf-8')
#             print(f"Data for batch {batch_number} saved to {output_file}.")
#             await problems(f"Data for batch {batch_number} saved to {output_file}.")
#
#             # Clear all_data for the next batch
#             all_data = []
#
#             # Sleep for 1 minute after each batch
#             print(f"Sleeping for 1 minute after batch {batch_number}...")
#             await asyncio.sleep(60)
#
#             batch_number += 1
#
# # Generate URLs for scraping (based on page IDs)
# url_list = [base_url.format(i=i) for i in range(0, 1514225)]  # Adjust the range as needed
# print(f"Total URLs to scrape: {len(url_list)}")
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
                df.to_csv(output_file, index=False, encoding='utf-8')
                print(f"Batch {batch_number} saved to {output_file}.")

            print(f"Sleeping for 1 minute after batch {batch_number}...")
            await asyncio.sleep(60)


url_list = [base_url.format(i=i) for i in range(2, 1514225)]

print(f"Total URLs to scrape: {len(url_list)}")

# Run the scraper
asyncio.run(scrape_urls(url_list))


# Optional: If you want to fetch and process a single URL separately
# async def main():
#     async with aiohttp.ClientSession() as session:  # Create the session within the async function
#         url = "https://uzorg.info/oz/info-id-1514224"
#         url, html_content = await fetch_url(session, url)  # Unpack the tuple
#         if html_content:  # Only proceed if html_content is not None
#             dynamic_headers, data = extract_headers_and_data(html_content)  # Pass the HTML content to the function
#             print(dynamic_headers, data)
#         else:
#             print(f"Failed to fetch content from {url}")
#
# # Run the main function for a single page
# asyncio.run(main())