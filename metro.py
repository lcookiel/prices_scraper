import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import csv
import logging
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed


# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def fetch_sitemap(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        logging.error(f'Failed to retrieve the sitemap: {e}')
        return None


def parse_sitemap(xml_content):
    soup = BeautifulSoup(xml_content, 'xml')
    urls = [loc.get_text() for loc in soup.find_all('loc')]
    return urls


def save_urls_to_csv(urls, filename):
    df = pd.DataFrame(urls, columns=['URL'])
    df.to_csv(filename, index=False)
    logging.info(f'URLs saved to {filename}')


def scrape_product_info(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    tries = 0
    max_tries = 3

    while tries < max_tries:
        tries += 1
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract the title
            title_element = soup.find('h1', {'data-marker': 'Big Product Cart Title'})
            title = title_element.get_text() if title_element else None

            # Extract the weight
            weight_element = soup.find('div', {'data-marker': 'Weight'})
            weight = weight_element.get_text() if weight_element else None

            # Extract the stock status
            stock_element = soup.find('div', {'data-testid': 'stock-balance-label', 'data-marker': 'Stock_balance_label'})
            stock = 'out'
            if stock_element:
                classes = stock_element.get('class', [])
                if 'BigProductStockBalanceLabel_in_stock' in classes:
                    stock = 'instock'
                elif 'BigProductStockBalanceLabel_running_out' in classes:
                    stock = 'low'

            # Extract the prices
            discounted_price_element = soup.find('span', {'data-marker': 'Discounted Price'})
            discounted_price = discounted_price_element.get_text() if discounted_price_element else None

            old_price_element = soup.find('span', {'data-marker': 'Old Price'})
            old_price = old_price_element.get_text() if old_price_element else discounted_price

            # Extract the trademark
            trademark_element = soup.find('li', {'data-marker': 'Taxon tm'})
            if trademark_element:
                span_elements = trademark_element.find_all('span')
                if len(span_elements) > 1:
                    trademark = span_elements[1].get_text()
                else:
                    trademark = None
            else:
                trademark = None

            # Extract the producer
            producer_element = soup.find('li', {'data-marker': 'Taxon pr'})
            if producer_element:
                span_elements = producer_element.find_all('span')
                if len(span_elements) > 1:
                    producer = span_elements[1].get_text()
                else:
                    producer = None
            else:
                producer = None

            # Extract the origin country
            origin_country_element = soup.find('li', {'data-marker': 'Taxon country'})
            if origin_country_element:
                span_elements = origin_country_element.find_all('span')
                if len(span_elements) > 1:
                    origin_country = span_elements[1].get_text()
                else:
                    origin_country = None
            else:
                origin_country = None

            # TODO: Extract the METRO product category
            # tag = soup.find_all("li", {"class": "jsx-2504a5e7448e00b2 Breadcrumbs__item icon-caret-bottom"})
            # print(tag)
            # product_category = tag.find("span").text

            # Return the data as a dictionary
            return {
                'URL': url,
                'Title': title,
                'Weight': weight,
                'Stock': stock,
                'Old Price': old_price,
                'Discounted Price': discounted_price,
                'Trademark': trademark,
                'Producer': producer,
                'Origin Country': origin_country #,
                # 'Product Category 1': product_category
            }
        except requests.RequestException as e:
            logging.error(f'Attempt {tries} failed to retrieve the page: {url} - {e}')
            if tries == max_tries:
                logging.error(f'All attempts to retrieve the page failed: {url}')
                return None


def get_processed_urls(output_filename):
    if os.path.exists(output_filename):
        df = pd.read_csv(output_filename)
        logging.debug(f'Columns in the output file: {df.columns.tolist()}')
        if 'URL' in df.columns:
            return set(df['URL'])
        else:
            logging.error(f'URL column not found in {output_filename}')
            return set()
    return set()


def save_batch_data(batch_data, output_filename):
    with open(output_filename, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=[
            'URL', 'Title', 'Weight', 'Stock', 'Old Price', 'Discounted Price', 
            'Trademark', 'Producer', 'Origin Country', 'Scrape Date'#, 'Product Category 1'
        ])
        writer.writerows(batch_data)
    logging.info(f'Saved a batch of {len(batch_data)} products to {output_filename}')


def scrape_all_products(sitemap_url, products_filename, output_filename, max_workers=5):
    sitemap_content = fetch_sitemap(sitemap_url)
    if sitemap_content:
        urls = parse_sitemap(sitemap_content)
        save_urls_to_csv(urls, products_filename)

        processed_urls = get_processed_urls(output_filename)
        total_urls = len(urls)
        remaining_urls = [url for url in urls if url not in processed_urls]

        # Initialize the CSV file with headers if starting from scratch
        if not os.path.exists(output_filename):
            with open(output_filename, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=[
                    'URL', 'Title', 'Weight', 'Stock', 'Old Price', 'Discounted Price', 
                    'Trademark', 'Producer', 'Origin Country', 'Scrape Date'#, 'Product Category 1'
                ])
                writer.writeheader()

        batch_size = 25
        batch_data = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(scrape_product_info, url): url for url in remaining_urls}
            completed_urls = len(processed_urls)

            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    data = future.result()
                    if data:
                        data['Scrape Date'] = datetime.now().strftime('%Y-%m-%d')
                        batch_data.append(data)
                        processed_urls.add(url)
                        completed_urls += 1
                        if len(batch_data) >= batch_size:
                            save_batch_data(batch_data, output_filename)
                            batch_data.clear()
                    progress = (completed_urls / total_urls) * 100
                    logging.info(f'Progress: {progress:.2f}% ({completed_urls}/{total_urls})')
                except Exception as e:
                    logging.error(f'Error scraping {url}: {e}')

        # Save any remaining data in the last batch
        if batch_data:
            save_batch_data(batch_data, output_filename)


if __name__ == "__main__":
    SITEMAP_URL = 'https://metro.zakaz.ua/products-sitemap-uk.xml'
    PRODUCTS_FILENAME = 'metro_products.csv'
    OUTPUT_FILENAME = 'product_info.csv'
    
    start_time = time.time()
    scrape_all_products(SITEMAP_URL, PRODUCTS_FILENAME, OUTPUT_FILENAME)
    end_time = time.time()
    logging.info(f'Scraping completed in {end_time - start_time:.2f} seconds')
