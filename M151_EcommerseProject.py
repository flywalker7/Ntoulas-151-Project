# Importing libraries
# Look "readme" for details
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

# Path to ChromeDriver.
chromedriver_path = r'C:\Users\Γιωργος\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe'

# Get the product description from the web page using selenium.
# Selenium was needed to recognize the tag of product description for cookshop.
def fetch_product_description_with_selenium(url, description_selector):
    # Initialization
    service = Service(chromedriver_path) 
    options = webdriver.ChromeOptions()
    options.add_argument("--headless") #run chrome in headless mode
    driver = webdriver.Chrome(service=service, options=options)
    
    driver.get(url) #"hit" the URL
    try:
        # Wait for the description tag to be found
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, description_selector))
        )
        soup = BeautifulSoup(driver.page_source, 'html.parser') #Parse the page
        description_tag = soup.select_one(description_selector) #Set the description tag
        # Extraction of description and debug
        if description_tag: 
            description_parts = description_tag.find_all('p')
            description = " ".join([part.get_text(strip=True) for part in description_parts])
            return description, str(soup)
        else:
            return "No description found", str(soup)
    finally:
        driver.quit() #End instance

# Get product details.
def extract_product_info(url, base_url, config):
    response = requests.get(url) #make a GET request to the url.
    # Dataframe creation. We use Dataframe to store all the data we get as seen below.
    products = pd.DataFrame(columns=["HTML", "Description", "URL", "Price", "Image Info", "Content HTML", "Source", "Title", "Availability", "Product Code", "Brand"])
    
    if response.status_code == 200: # Ensure that the request was a success.
        soup = BeautifulSoup(response.content, 'html.parser') #Content parsing.
        print(f"Fetched content from {url}") #This is kept to help us find out in which shop the code was "breaking".

        product_items = soup.select(config['product_list'])
        rows = [] #We use a list to temporarily store the data. Later we will feed the dataframe with the data in the rows list.
        
        # Now we begin the data extraction.
        # We follow two different methods. We had some initial values and then we decided to add some more to make the result more appealing and we we inconsistent with our code.
        for product in product_items:
            html_content = str(product)
            description_tag = product.select_one(config['description'])
            description = description_tag.text.strip() if description_tag else 'No description'

            # Given the different structure of the selected webpages we were forced to take into account many cases.
            # This issue is very vibrant in the case of the coockshop web page

            # Here we get the link tag
            if config['site'] == 'cookshop':
                link_tag = product.find('a', class_='wrap')
            else:
                if description_tag:
                    link_tag = description_tag.find('a') 
                else:
                    None

            # Here we construct the link tag. Again many cases were taken into account to fix all the possible issues.
            relative_link = link_tag['href'] if link_tag else''
            if relative_link.startswith('/'):
                full_link = base_url + relative_link
            elif relative_link.startswith('http'):
                full_link = relative_link
            elif relative_link:
                full_link = base_url + '/' + relative_link
            else:
                full_link = base_url

            # Price extraction
            price_tag = product.select_one(config['price'])
            price = price_tag.text.strip() if price_tag else 'No price'

            # Image extraction
            image_tag = product.select_one(config['image'])
            if image_tag:
                image_info = image_tag['src']
                if image_info.startswith('//'):
                    image_info = 'https:' + image_info
                elif image_info.startswith('/'):
                    image_info = base_url + image_info
            else:
                image_info = 'No image found'

            # Initilize the values of the second batch of fields.
            # Here we don't take all the cases directly when looking for the tags but we seperate from the start the shops and set things right from the beginning.
            title = 'No title'
            availability = 'No availability info'
            product_code = 'No product code'
            brand = 'No brand'
            product_page_html = 'No product page HTML'

            # We start with coockshop.
            if config['site'] == 'cookshop':
                description, product_page_html = fetch_product_description_with_selenium(full_link, config['product_page']['description'])

                # Title extraction
                title_tag = BeautifulSoup(product_page_html, 'html.parser').select_one(config['product_page']['title'])                
                if title_tag:
                    title = title_tag.text.strip()
                else:
                    title = 'No title'

                # Availability extraction    
                availability_tag = BeautifulSoup(product_page_html, 'html.parser').select_one(config['product_page']['availability'])
                if availability_tag:
                    availability = availability_tag.text.strip()
                else:
                    availability = 'No availability info'

                # Product code extraction    
                product_code_tag = BeautifulSoup(product_page_html, 'html.parser').select_one(config['product_page']['product_code'])
                if product_code_tag:
                    product_code = product_code_tag.text.strip()
                else:
                    product_code = 'No product code'

                # Brand extraction
                brand_tag = BeautifulSoup(product_page_html, 'html.parser').select_one(config['brand'])
                if brand_tag:
                    brand = brand_tag.text.strip()
                else:
                    brand = 'No brand'
            # Now we go for the rest of the shops
            else:
                product_response = requests.get(full_link)
                if product_response.status_code == 200:
                    product_page_html = BeautifulSoup(product_response.content, 'html.parser')
                    
                    title_tag = product_page_html.select_one(config['product_page']['title'])
                    # Title extraction
                    if title_tag:
                        title = title_tag.text.strip() 
                    else:
                        title = 'No title'
                        
                    # Description extraction
                    description_tag = product_page_html.select_one(config['product_page']['description'])
                    if description_tag:
                        description = description_tag.text.strip()
                    else:
                        description = 'No description'
                      
                    # For the availability we had to take again two cases.
                    # Availability extraction
                    if config['site'] == 'e-druster':
                        availability_tag = product_page_html.select_one("span[style*='font-size:11px;display:inline-block;color:green;margin-left: -5px;margin-top: 5px']")
                        if availability_tag:
                            availability = availability_tag.text.strip()
                        else:
                            availability = 'No availability info'
                    else:
                        availability_tag = product_page_html.select_one(config['product_page']['availability'])
                        if availability_tag:
                            availability = availability_tag.text.strip() 
                        else:
                            availability = 'No availability info'
                          
                    # Product code extraction
                    # Here we took into account cases where the information about brand is optional or/and not provided
                    product_code_tag = product_page_html.select_one(config['product_page']['product_code'])
                    if product_code_tag:
                        product_code = product_code_tag.text.strip() 
                    else:
                        product_code = 'No product code'

                    if 'brand' in config:
                        brand_tag = product.select_one(config['brand'])
                        if brand_tag:
                            brand = brand_tag.text.strip() 
                        else:
                            brand = 'No brand'
                    else:
                        brand = 'No brand'
                else:
                    product_page_html = 'failed to retrieve content'

            # Append product details in the rows list.
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

        # Insert product rows in the dataFrame.
        products = pd.concat([products, pd.DataFrame(rows)], ignore_index=True)
    else:
        print(f"failed to retrieve the page, status code {response.status_code}")

    return products 

# Store data in a MySQL database
def store_data_in_mysql(data, table_name, db_config):
    try:
        connection = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )

        # Create a cursor to parse the table
        if connection.is_connected():
            cursor = connection.cursor()

            # Create a table if it doesn't exist
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

            # Update or enter new data in the table
            for _, row in data.iterrows():
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
                
                # Convert BeautifulSoup objects to strings in order to be able to store them the MySQL base
                row = row.apply(lambda x: str(x) if isinstance(x, BeautifulSoup) else x)  
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
    # We set dictionaries as we thought it make it easier to expand the code in the future if more shops are to be added.
    # These dictionaries contain the tags that are being used to locate the products info we wanted.
    # Some of these info are located in the PLP (generic page) and some of them in the PDP (product page)
    # "fetch_product_destription_with_selenium" fuction takes 2 urls as parameters, here it is becoming even more clear.
    # He have a base and PLP url for every shop.
    
    url1 = "https://www.e-dructer.com/mikrosiskeves/skoupes/skoupakia/"
    base_url1 = "https://www.e-dructer.com"
    
    config1 = {
        "product_list": "div.grid-list div.ty-grid-list__item",
        "description": "div.ty-grid-list__item-name",
        "price": "span.ty-price-num",
        "image": "img.ty-pict", 
        "brand": "strong.brando",
        "site": "e-druster",
        "product_page": {
            "title": "h1.ty-product-block-title",
            "description": "div.perigrafi_gar",
            "availability": "span[style*='font-size:11px;display:inline-block;color:green;margin-left: -5px;margin-top: 5px']",
            "product_code": "span.ty-control-group__item"
        }
    }
    
    url2 = "https://cosmomarket.gr/c/005423910436/Ilektrikes_Skoupes.html"
    base_url2 = "https://cosmomarket.gr"
    
    config2 = {
        "product_list": "div.col-6.col-md-4.col-xl-3",
        "description": "h2.product-title",
        "price": "span.product-price",
        "image": "img",
        "site": "cosmomarket",
        "product_page": {
            "title": "h1.product-title", 
            "description": "div.product-desc-content",
            "availability": "div.category-list[style*='font-size: 1.3rem;margin-bottom:10px;']",
            "product_code": "div.category-list[style*='font-size: 1.3rem;']"
        }
    }
    
    url3 = "https://www.cookshop.gr/el/catalog/%CF%83%CE%BA%CE%B5%CF%85%CE%B7-%CE%BC%CE%B1%CE%B3%CE%B5%CE%B9%CF%81%CE%B9%CE%BA%CE%B7%CF%83-%CF%84%CE%B7%CE%B3%CE%B1%CE%BD%CE%B9%CE%B1?fbclid=IwZXh0bgNhZW0CMTAAAR2DIGFZexilB07-KwRSMuPWp_NcOymif4X85_aqCUqyevz6nb72V9-JiGk_aem_AVQPsqR5BIQ0gaO-1JPFc4EfJChrYciD_Lb-iNNXCkXeeY4bcBggV5ResWqTtIocVxfomGYw-R3KE7Ul_I9gCTsr" 
    base_url3 = "https://www.cookshop.gr"
    
    config3 = {
        "product_list": "ul.product-list div.gya-product",
        "description": "div.description",
        "price": "div.price", 
        "image": "img",
        "site": "cookshop", 
        "product_page": {
            "title": "h1.page-title.center",
            "description": "div.tab.active-now#group-0",
            "availability": "span.avail",
            "product_code": "span.code"
        },
        "brand": "div.brand-title"
    }

    url4 = "https://www.cookshop.gr/el/catalog/%CE%B7%CE%BB%CE%B5%CE%BA%CF%84%CF%81%CE%B9%CE%BA%CE%B5%CF%83-%CE%BC%CE%B9%CE%BA%CF%81%CE%BF%CF%83%CF%85%CF%83%CE%BA%CE%B5%CF%85%CE%B5%CF%83-air?fbclid=IwZXh0bgNhZW0CMTAAAR0mQLgxj3npDbsytVytAc8fwX6D307Yu7jGSGXQ-EvpUT6cSSQujs68nCk_aem_AV7JIfZ04Y8EsR-2WULoAgrj2hYMX_WSSiG_iYz7hGsAdY5SeX-zJ3gk4uRrf7EJY0TEuE0Uh9ajEXisjCY4J6tT"
    base_url4 = "https://www.cookshop.gr"

    config4 = {
        "product_list": "ul.product-list div.gya-product",
        "description": "div.description",
        "price": "div.price",
        "image": "img",
        "site": "cookshop",
        "product_page": {
            "title": "h1.page-title.center",
            "description": "div.tab.active-now#group-0",
            "availability": "span.avail",
            "product_code": "span.code"
        },
        "brand": "div.brand-title"
    }
    
    url5 = "https://www.e-dructer.com/mikrosiskeves/proino/kafetieres/?features_hash=85-2661&fbclid=IwZXh0bgNhZW0CMTAAAR06GMq-yoXfUqzND_7X4ZZ5Icf6NiH10Dpf9npODGuItwSlon0qI1dwxQA_aem_AV6RNN3-q4Z7ZeRrxUIVMrNzZrW3NV2PF5ctMOi3cJXm_8cvQ5Fd4IoxCTUNDp95tXJyrhDpn_q4QHXavo9RZjAS"
    base_url5 = "https://www.e-dructer.com"
    
    config5 = {
        "product_list": "div.grid-list div.ty-grid-list__item",
        "description": "div.ty-grid-list__item-name",
        "price": "span.ty-price-num",
        "image": "img.ty-pict", 
        "brand": "strong.brando",
        "site": "e-druster",
        "product_page": {
            "title": "h1.ty-product-block-title",
            "description": "div.perigrafi_gar",
            "availability": "span[style*='font-size:11px;display:inline-block;color:green;margin-left: -5px;margin-top: 5px']",
            "product_code": "span.ty-control-group__item"
        }
    }

    # Here we set the "final" dataFrame that will be feeded by all the pages. 
    all_products = pd.DataFrame(columns=["HTML", "Description", "URL", "Price", "Image Info", "Content HTML", "Source", "Title", "Availability", "Product Code", "Brand"]) 
    
    product_info1 = extract_product_info(url1, base_url1, config1)
    all_products = pd.concat([all_products, product_info1], ignore_index=True)
    
    product_info2 = extract_product_info(url2, base_url2, config2)
    all_products = pd.concat([all_products, product_info2], ignore_index=True)
    
    product_info3 = extract_product_info(url3, base_url3, config3)
    all_products = pd.concat([all_products, product_info3], ignore_index=True)

    product_info4 = extract_product_info(url4, base_url4, config4)
    all_products = pd.concat([all_products, product_info4], ignore_index=True)

    product_info5 = extract_product_info(url5, base_url5, config5)
    all_products = pd.concat([all_products, product_info5], ignore_index=True)

    # Database credentials
    db_config = {
        "host": "localhost",
        "user": "root",
        "password": "admin",
        "database": "ntoulasBase"
    }
    
    table_name = "product_data"

    # Final command to in order to store all the data in the database.
    store_data_in_mysql(all_products, table_name, db_config)
