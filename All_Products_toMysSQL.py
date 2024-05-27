import requests
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import mysql.connector
from mysql.connector import Error

# Path to your ChromeDriver executable
chromedriver_path = r'C:\Users\charisis\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe'

def fetch_product_description_with_selenium(url, description_selector):
    # Set up the Selenium web driver
    service = Service(chromedriver_path)
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Run in headless mode (without opening a browser window)
    driver = webdriver.Chrome(service=service, options=options)

    # Fetch the page and wait for the content to load
    driver.get(url)
    try:
        # Wait until the specific element is present
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, description_selector))
        )
        # Extract the page source after JavaScript execution
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        description_tag = soup.select_one(description_selector)
        if description_tag:
            description_parts = description_tag.find_all('p')
            description = " ".join([part.get_text(strip=True) for part in description_parts])
            return description, str(soup)
        else:
            return "No description found", str(soup)
    finally:
        driver.quit()

def extract_product_info(url, base_url, config):
    # Function to fetch a webpage and extract product information.
    response = requests.get(url)
    products = pd.DataFrame(columns=["HTML", "Description", "URL", "Price", "Image Info", "Content HTML", "Source", "Title", "Availability", "Product Code", "Brand"])

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        print(f"Fetched content from {url}")

        product_items = soup.select(config['product_list'])
        rows = []

        for product in product_items:
            html_content = str(product)
            description_tag = product.select_one(config['description'])
            description = description_tag.text.strip() if description_tag else 'No description'

            # Site-specific URL extraction logic
            if config['site'] == 'cookshop':
                link_tag = product.find('a', class_='wrap')
            else:
                link_tag = description_tag.find('a') if description_tag else None

            relative_link = link_tag['href'] if link_tag else ''
            if relative_link.startswith('/'):
                full_link = base_url + relative_link
            elif relative_link.startswith('http'):
                full_link = relative_link
            elif relative_link:
                full_link = base_url + '/' + relative_link
            else:
                full_link = base_url

            price_tag = product.select_one(config['price'])
            price = price_tag.text.strip() if price_tag else 'No price'

            image_tag = product.select_one(config['image'])
            if image_tag:
                image_info = image_tag['src']
                if image_info.startswith('//'):
                    image_info = 'https:' + image_info
                elif image_info.startswith('/'):
                    image_info = base_url + image_info
            else:
                image_info = 'No image found'

            # Initialize all variables to default values
            title = 'No title'
            availability = 'No availability info'
            product_code = 'No product code'
            brand = 'No brand'
            product_page_html = 'No product page HTML'

            # Fetch HTML content of the product's individual page
            if config['site'] == 'cookshop':
                description, product_page_html = fetch_product_description_with_selenium(full_link, config['product_page']['description'])
                title_tag = BeautifulSoup(product_page_html, 'html.parser').select_one(config['product_page']['title'])
                title = title_tag.text.strip() if title_tag else 'No title'
                availability_tag = BeautifulSoup(product_page_html, 'html.parser').select_one(config['product_page']['availability'])
                availability = availability_tag.text.strip() if availability_tag else 'No availability info'
                product_code_tag = BeautifulSoup(product_page_html, 'html.parser').select_one(config['product_page']['product_code'])
                product_code = product_code_tag.text.strip() if product_code_tag else 'No product code'
                brand_tag = BeautifulSoup(product_page_html, 'html.parser').select_one(config['brand'])
                brand = brand_tag.text.strip() if brand_tag else 'No brand'
            else:
                product_response = requests.get(full_link)
                if product_response.status_code == 200:
                    product_page_html = BeautifulSoup(product_response.content, 'html.parser')
                    title_tag = product_page_html.select_one(config['product_page']['title'])
                    title = title_tag.text.strip() if title_tag else 'No title'

                    description_tag = product_page_html.select_one(config['product_page']['description'])
                    description = description_tag.text.strip() if description_tag else 'No description'

                    if config['site'] == 'e-dructer':
                        availability_tag = product_page_html.select_one("span[style*='font-size:11px;display:inline-block;color:green;margin-left: -5px;margin-top: 5px']")
                        availability = availability_tag.text.strip() if availability_tag else 'No availability info'
                    else:
                        availability_tag = product_page_html.select_one(config['product_page']['availability'])
                        availability = availability_tag.text.strip() if availability_tag else 'No availability info'

                    product_code_tag = product_page_html.select_one(config['product_page']['product_code'])
                    product_code = product_code_tag.text.strip() if product_code_tag else 'No product code'

                    if 'brand' in config:
                        brand_tag = product.select_one(config['brand'])
                        brand = brand_tag.text.strip() if brand_tag else 'No brand'
                    else:
                        brand = 'No brand'
                else:
                    product_page_html = 'Failed to retrieve content'

            rows.append({
                "HTML": html_content,
                "Description": description,
                "URL": full_link,
                "Price": price,
                "Image Info": image_info,
                "Content HTML": product_page_html,
                "Source": base_url,
                "Title": title,
                "Availability": availability,
                "Product Code": product_code,
                "Brand": brand
            })

        products = pd.concat([products, pd.DataFrame(rows)], ignore_index=True)
    else:
        print(f"Failed to retrieve the page, status code {response.status_code}")

    return products

def store_data_in_mysql(data, table_name, db_config):
    # Function to store data in a MySQL database.
    try:
        connection = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # Create table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                HTML TEXT,
                Description TEXT,
                URL VARCHAR(2083),
                Price VARCHAR(255),
                Image_Info TEXT,
                Content_HTML LONGTEXT,
                Source VARCHAR(255),
                Title VARCHAR(255),
                Availability VARCHAR(255),
                Product_Code VARCHAR(255),
                Brand VARCHAR(255),
                UNIQUE KEY unique_url (URL(255))
            );
            """
            cursor.execute(create_table_query)

            # Insert or update data in the table
            for _, row in data.iterrows():
                # Use ON DUPLICATE KEY UPDATE to insert a new record or update the existing one
                insert_update_query = f"""
                INSERT INTO {table_name} (HTML, Description, URL, Price, Image_Info, Content_HTML, Source, Title, Availability, Product_Code, Brand)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    HTML=VALUES(HTML),
                    Description=VALUES(Description),
                    Price=VALUES(Price),
                    Image_Info=VALUES(Image_Info),
                    Content_HTML=VALUES(Content_HTML),
                    Source=VALUES(Source),
                    Title=VALUES(Title),
                    Availability=VALUES(Availability),
                    Product_Code=VALUES(Product_Code),
                    Brand=VALUES(Brand);
                """
                cursor.execute(insert_update_query, tuple(row))

            connection.commit()
            print(f"Data has been stored in the table '{table_name}' in the database.")

    except Error as e:
        print(f"Error while connecting to MySQL: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

if __name__ == "__main__":
    # Configuration for the first website (e-dructer)
    url1 = "https://www.e-dructer.com/mikrosiskeves/skoupes/skoupakia/"
    base_url1 = "https://www.e-dructer.com"
    
    config1 = {
        "product_list": "div.grid-list div.ty-grid-list__item",  # CSS selector for the product list container
        "description": "div.ty-grid-list__item-name",            # CSS selector for the product description container
        "price": "span.ty-price-num",                            # CSS selector for the product price
        "image": "img.ty-pict",                                  # CSS selector for the product image
        "brand": "strong.brando",                                # CSS selector for the brand on the main page
        "site": "e-dructer",                                     # Site identifier
        "product_page": {
            "title": "h1.ty-product-block-title",                # CSS selector for the product page title
            "description": "div.perigrafi_gar",                  # CSS selector for the product page description
            "availability": "span[style*='font-size:11px;display:inline-block;color:green;margin-left: -5px;margin-top: 5px']",  # CSS selector for the product page availability
            "product_code": "span.ty-control-group__item"        # CSS selector for the product page product code
        }
    }

    # Configuration for the second website (cosmomarket)
    url2 = "https://cosmomarket.gr/c/005423910436/Ilektrikes_Skoupes.html"
    base_url2 = "https://cosmomarket.gr"
    
    config2 = {
        "product_list": "div.col-6.col-md-4.col-xl-3",           # CSS selector for the product list container
        "description": "h2.product-title",                       # CSS selector for the product description container
        "price": "span.product-price",                           # CSS selector for the product price
        "image": "img",                                          # CSS selector for the product image
        "site": "cosmomarket",                                   # Site identifier
        "product_page": {
            "title": "h1.product-title",                         # CSS selector for the product page title
            "description": "div.product-desc-content",           # CSS selector for the product page description
            "availability": "div.category-list[style*='font-size: 1.3rem;margin-bottom:10px;']",  # CSS selector for the product page availability
            "product_code": "div.category-list[style*='font-size: 1.3rem;']"                      # CSS selector for the product page product code
        }
    }

    # Configuration for the third website (cookshop)
    url3 = "https://www.cookshop.gr/el/catalog/%CF%83%CE%BA%CE%B5%CF%85%CE%B7-%CE%BC%CE%B1%CE%B3%CE%B5%CE%B9%CF%81%CE%BA%CE%B7%CF%83-%CF%84%CE%B7%CE%B3%CE%B1%CE%BD%CE%B9%CE%B1"
    base_url3 = "https://www.cookshop.gr"
    
    config3 = {
        "product_list": "ul.product-list div.gya-product",       # CSS selector for the product list container
        "description": "div.description",                        # CSS selector for the product description container
        "price": "div.price",                                    # CSS selector for the product price
        "image": "img",                                          # CSS selector for the product image
        "site": "cookshop",                                      # Site identifier
        "product_page": {
            "title": "h1.page-title.center",                     # CSS selector for the product page title
            "description": "div.tab.active-now#group-0",         # CSS selector for the product page description
            "availability": "span.avail",                        # CSS selector for the product page availability
            "product_code": "span.code"                          # CSS selector for the product page product code
        },
        "brand": "div.brand-title"                               # CSS selector for the brand
    }

    # Initialize an empty DataFrame to hold all product information
    all_products = pd.DataFrame(columns=["HTML", "Description", "URL", "Price", "Image Info", "Content HTML", "Source", "Title", "Availability", "Product Code", "Brand"])

    # Extract product information for the first website (e-dructer)
    product_info1 = extract_product_info(url1, base_url1, config1)
    all_products = pd.concat([all_products, product_info1], ignore_index=True)

    # Extract product information for the second website (cosmomarket)
    product_info2 = extract_product_info(url2, base_url2, config2)
    all_products = pd.concat([all_products, product_info2], ignore_index=True)

    # Extract product information for the third website (cookshop)
    product_info3 = extract_product_info(url3, base_url3, config3)
    all_products = pd.concat([all_products, product_info3], ignore_index=True)

    # Define the database configuration
    db_config = {
        "host": "localhost",
        "user": "root",
        "password": "admin",
        "database": "ntoulasBase"
    }

    # Define the table name for storing the product information
    table_name = "product_data"

    # Store the combined product information in the MySQL database
    store_data_in_mysql(all_products, table_name, db_config)
