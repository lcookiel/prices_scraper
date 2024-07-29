import cloudscraper
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

# Create a cloudscraper instance
scraper = cloudscraper.create_scraper()

def fetch_sitemap(url):
    try:
        response = scraper.get(url)
        # response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        logging.error(f'Failed to retrieve the sitemap: {e}')
        return None


def parse_sitemap(xml_content):
    soup = BeautifulSoup(xml_content, 'xml')
    urls = [loc.get_text() for loc in soup.find_all('loc')]
    return urls


def scrape_product_info(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    tries = 0
    max_tries = 3

    while tries < max_tries:
        tries += 1
        try:
            response = scraper.get(url, headers=headers)
            # response = requests.get(url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract the title
            title_element = soup.find('h1', {'class': 'page-title'})
            title = title_element.get_text() if title_element else None

            # Extract the price unit
            price_unit_element = soup.find('span', {'class': 'product-price__unit'})
            price_unit = price_unit_element.get_text().replace('/', '').strip() if price_unit_element else None

            # Find the product characteristics table for 'weight', 'trademark', and 'origin country'
            characteristic_items = soup.find_all('div', class_='product-characteristics__item')

            # Extract the weight
            weight = None
            for item in characteristic_items:
                name_element = item.find('div', class_='product-characteristics__name')
                if name_element:
                    name_text = name_element.get_text()
                    value_element = item.find('div', class_='product-characteristics__value')
                    if value_element:
                        value_text = value_element.get_text()
                        if name_text == 'Вага':
                            weight = value_text
                            break  # Prioritize "Вага" and exit loop
                        elif name_text == "Об’єм" and weight is None:
                            weight = value_text

            # Extract the trademark
            trademark = None
            for item in characteristic_items:
                name_element = item.find('div', class_='product-characteristics__name')
                if name_element and name_element.get_text() == 'Торгова марка':
                    value_element = item.find('div', class_='product-characteristics__value')
                    if value_element and value_element.a:
                        trademark = value_element.a.get_text()
                    break

            # Extract the origin country
            origin_country = None
            for item in characteristic_items:
                name_element = item.find('div', class_='product-characteristics__name')
                if name_element and name_element.get_text() == 'Країна':
                    value_element = item.find('div', class_='product-characteristics__value')
                    if value_element:
                        origin_country = value_element.get_text()
                    break

            # Extract the stock status
            stock_element = soup.find('span', {'class': 'available-tag__text'}).get_text()
            stock = 'out'
            if stock_element == 'Є в наявності':
                stock = 'in'
            elif stock_element == 'Закінчується':
                stock = 'low' # or 'very low'?

            # Extract the prices
            product_price_div = soup.find('div', class_='product-about__price')
            # Initialize prices
            discounted_price = None
            old_price = None
            if product_price_div:
                # Check for the presence of product-price__bottom for old price
                old_price_element = product_price_div.find('data', {'class': 'product-price__bottom'})
                if old_price_element:
                    # Extract old price from product-price__bottom
                    old_price_span = old_price_element.find('span')
                    old_price = old_price_span.get_text() if old_price_span else None
                    
                    # Extract discounted price from product-price__top
                    discounted_price_element = product_price_div.find('data', {'class': 'product-price__top'})
                    discounted_price_span = discounted_price_element.find('span') if discounted_price_element else None
                    discounted_price = discounted_price_span.get_text() if discounted_price_span else None
                else:
                    # If no discounted price, treat product-price__top as old price
                    old_price_element = product_price_div.find('data', {'class': 'product-price__top'})
                    old_price_span = old_price_element.find('span') if old_price_element else None
                    old_price = old_price_span.get_text() if old_price_span else None

            # Return the data as a dictionary
            return {
                'url': url,
                'title': title,
                'weight': weight,
                'stock': stock,
                'old_price': old_price,
                'discounted_price': discounted_price,
                'price_unit': price_unit,
                'trademark': trademark,
                'origin_country': origin_country
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
        if 'url' in df.columns:
            return set(df['url'])
        else:
            logging.error(f'url column not found in {output_filename}')
            return set()
    return set()


def save_batch_data(batch_data, output_filename):
    with open(output_filename, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=[
            'url', 'title', 'weight', 'stock', 'old_price', 'discounted_price', 
            'trademark', 'price_unit', 'origin_country', 'scrape_date'
        ])
        writer.writerows(batch_data)
    logging.info(f'Saved a batch of {len(batch_data)} products to {output_filename}')


def scrape_all_products(sitemap_url, output_filename, max_workers=5):
    sitemap_content = fetch_sitemap(sitemap_url)
    if sitemap_content:
        urls = parse_sitemap(sitemap_content)

        processed_urls = get_processed_urls(output_filename)
        total_urls = len(urls)
        remaining_urls = [url for url in urls if url not in processed_urls]

        # Record the number of total urls to a separate file (for server running purposes)
        with open("n_rows_atb.txt", 'w') as f:
            f.write(str(total_urls))

        # Initialize the CSV file with headers if starting from scratch
        if not os.path.exists(output_filename):
            with open(output_filename, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=[
                    'url', 'title', 'weight', 'stock', 'old_price', 'discounted_price', 
                    'trademark', 'price_unit', 'origin_country', 'scrape_date'
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
                        data['scrape_date'] = datetime.now().strftime('%Y-%m-%d')
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
    SITEMAP_URL = 'https://www.atbmarket.com/sitemap_products.xml'
    current_date = datetime.now().strftime('%Y%m%d')
    OUTPUT_FILENAME = f'atb{current_date}.csv'
    
    start_time = time.time()
    scrape_all_products(SITEMAP_URL, OUTPUT_FILENAME)
    end_time = time.time()
    logging.info(f'Scraping completed in {end_time - start_time:.2f} seconds')