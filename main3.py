import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

api_key = "400ca7a2f1867dfd87570a6053393443"
base_url = "https://uzorg.info/oz/info-id-{i}"  # Main page URL pattern

# Create a semaphore to limit concurrent requests
semaphore = asyncio.Semaphore(5)  # Limits to 5 concurrent requests

async def fetch_url(session, url):
    """Fetch a URL asynchronously with rate limiting."""
    async with semaphore:
        try:
            async with session.get(f"https://api.scraperapi.com?api_key={api_key}&url={url}") as response:
                await asyncio.sleep(1)  # Rate limit to avoid sending requests too quickly
                response.raise_for_status()
                html_content = await response.text()
                print(f"Fetched data from {url}")
                return url, html_content
        except Exception as e:
            print(f"Failed to fetch {url}: {e}")
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
            else:
                print(f"Failed to fetch {url}.")

# Generate URLs for scraping (based on page IDs)
url_list = [base_url.format(i=i) for i in range(400000, 600000)]  # Adjust the range as needed
print(url_list)

output_file = "scraped_data.csv"
asyncio.run(scrape_urls(url_list, output_file))