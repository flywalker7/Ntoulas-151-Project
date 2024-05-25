#!/usr/bin/env python
# coding: utf-8

# Import necessary modules for web requests, HTML parsing, data manipulation, and MySQL connection.
import requests
from bs4 import BeautifulSoup
import pandas as pd
import mysql.connector
from mysql.connector import Error

def extract_product_info(url, base_url, config):
    # Function to fetch a webpage and extract product information.
    # url: The specific webpage URL to scrape.
    # base_url: The main website URL used to construct full URLs from relative links.
    # config: A dictionary containing CSS selectors for various elements on the webpage.

    response = requests.get(url)
    # Send an HTTP GET request to the specified URL and store the response.
    # The response contains the HTML content of the webpage.

    products = pd.DataFrame(columns=["HTML", "Description", "URL", "Price", "Image Info", "Content HTML", "Source"])
    # Create an empty DataFrame with specified columns to store product information.
    # Added "Source" column to identify the source website.

    if response.status_code == 200:
        # Check if the request was successful (status code 200).
        soup = BeautifulSoup(response.content, 'html.parser')
        # Parse the HTML content using BeautifulSoup.

        print(f"Fetched content from {url}")
        # Print a message indicating that the content was fetched successfully.

        product_items = soup.select(config['product_list'])
        # Find the product list container using the CSS selector from the config dictionary.
        # This selects all product items within the specified container.

        rows = []
        # Initialize a list to store extracted product information.

        for product in product_items:
            # Loop through each product item in the product list.

            html_content = str(product)
            # Convert the product HTML content to a string.

            description_tag = product.select_one(config['description'])
            # Find the description tag within the product item using the CSS selector from the config dictionary.

            description = description_tag.text.strip() if description_tag else 'No description'
            # Extract and clean the product description text.

            # Site-specific URL extraction logic
            if config['site'] == 'cookshop':
                link_tag = product.find('a', class_='wrap')
            else:
                link_tag = description_tag.find('a') if description_tag else None

            relative_link = link_tag['href'] if link_tag else ''
            # Construct the full URL by combining the base URL and relative link if needed.
            if relative_link.startswith('/'):
                full_link = base_url + relative_link
            elif relative_link.startswith('http'):
                full_link = relative_link
            elif relative_link:
                full_link = base_url + '/' + relative_link
            else:
                full_link = base_url

            price_tag = product.select_one(config['price'])
            # Find the price tag within the product item using the CSS selector from the config dictionary.

            price = price_tag.text.strip() if price_tag else 'No price'
            # Extract and clean the product price text.

            image_tag = product.select_one(config['image'])
            # Find the image tag within the product item using the CSS selector from the config dictionary.

            if image_tag:
                image_info = image_tag['src']
                # Extract the image URL.
                
                # Check if the image URL is relative or protocol-relative
                if image_info.startswith('//'):
                    image_info = 'https:' + image_info
                elif image_info.startswith('/'):
                    image_info = base_url + image_info
            else:
                image_info = 'No image found'

            # Fetch HTML content of the product's individual page
            product_response = requests.get(full_link)
            # Send an HTTP GET request to the product's individual page URL.
            if product_response.status_code == 200:
                product_page_html = product_response.content
                # Store the HTML content of the product's individual page.
            else:
                product_page_html = 'Failed to retrieve content'
                # If the request failed, store a failure message.

            # Append the extracted product information as a dictionary to the rows list.
            rows.append({
                "HTML": html_content,
                "Description": description,
                "URL": full_link,
                "Price": price,
                "Image Info": image_info,
                "Content HTML": product_page_html,
                "Source": base_url
            })

        products = pd.concat([products, pd.DataFrame(rows)], ignore_index=True)
        # Concatenate the rows list (converted to a DataFrame) to the products DataFrame, ignoring the index.
        # This combines all the product data into a single DataFrame.

    else:
        print(f"Failed to retrieve the page, status code {response.status_code}")
        # If the request failed, print a message with the status code.
        # This informs us that the request was not successful and provides the reason.

    return products
    # Return the products DataFrame with the extracted product information.
    # This DataFrame can then be used for further analysis or export.

def store_data_in_mysql(data, table_name, db_config):
    # Function to store data in a MySQL database.
    # data: The DataFrame containing the data to be stored.
    # table_name: The name of the table where the data will be stored.
    # db_config: A dictionary containing database connection parameters.
    # The purpose of this function is to store the given data in a MySQL database.

    try:
        connection = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )
        # Establish a connection to the MySQL database using the provided configuration.

        if connection.is_connected():
            cursor = connection.cursor()
            # Create a cursor object to interact with the database.

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
                UNIQUE KEY unique_url (URL(255))
            );
            """
            cursor.execute(create_table_query)
            # Execute the query to create the table with specified columns if it doesn't exist.

            # Insert or update data in the table
            for _, row in data.iterrows():
                # Use ON DUPLICATE KEY UPDATE to insert a new record or update the existing one
                insert_update_query = f"""
                INSERT INTO {table_name} (HTML, Description, URL, Price, Image_Info, Content_HTML, Source)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    HTML=VALUES(HTML),
                    Description=VALUES(Description),
                    Price=VALUES(Price),
                    Image_Info=VALUES(Image_Info),
                    Content_HTML=VALUES(Content_HTML),
                    Source=VALUES(Source);
                """
                cursor.execute(insert_update_query, tuple(row))
            # Iterate through the DataFrame rows and insert each row into the database.
            # If a row with the same URL already exists, update its content with new values.

            connection.commit()
            # Commit the transaction to save the changes.

            print(f"Data has been stored in the table '{table_name}' in the database.")
            # Print a message indicating that the data has been successfully stored in the database.

    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        # Print an error message if there was an issue connecting to the database.

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            # Close the cursor and connection to the database.
            print("MySQL connection is closed")
            # Print a message indicating that the connection to the database has been closed.

if __name__ == "__main__":
    # Check if this script is being run as the main program (not being imported as a module).
    # If true, execute the following code block.

    # Configuration for the first website (e-dructer)
    url1 = "https://www.e-dructer.com/mikrosiskeves/skoupes/skoupakia/"
    base_url1 = "https://www.e-dructer.com"
    
    config1 = {
        "product_list": "div.grid-list div.ty-grid-list__item",  # CSS selector for the product list container
        "description": "div.ty-grid-list__item-name",            # CSS selector for the product description container
        "price": "span.ty-price-num",                            # CSS selector for the product price
        "image": "img.ty-pict",                                  # CSS selector for the product image
        "site": "e-dructer"                                      # Site identifier
    }

    # Configuration for the second website (cosmomarket)
    url2 = "https://cosmomarket.gr/c/005423910436/Ilektrikes_Skoupes.html"
    base_url2 = "https://cosmomarket.gr"
    
    config2 = {
        "product_list": "div.col-6.col-md-4.col-xl-3",           # CSS selector for the product list container
        "description": "h2.product-title",                       # CSS selector for the product description container
        "price": "span.product-price",                           # CSS selector for the product price
        "image": "img",                                          # CSS selector for the product image
        "site": "cosmomarket"                                    # Site identifier
    }

    # Configuration for the third website (cookshop)
    url3 = "https://www.cookshop.gr/el/catalog/%CF%83%CE%BA%CE%B5%CF%85%CE%B7-%CE%BC%CE%B1%CE%B3%CE%B5%CE%B9%CF%81%CE%B9%CE%BA%CE%B7%CF%83-%CF%84%CE%B7%CE%B3%CE%B1%CE%BD%CE%B9%CE%B1"
    base_url3 = "https://www.cookshop.gr"
    
    config3 = {
        "product_list": "ul.product-list div.gya-product",       # CSS selector for the product list container
        "description": "div.description",                        # CSS selector for the product description container
        "price": "div.price",                                    # CSS selector for the product price
        "image": "img",                                          # CSS selector for the product image
        "site": "cookshop"                                       # Site identifier
    }

    # Initialize an empty DataFrame to hold all product information
    all_products = pd.DataFrame(columns=["HTML", "Description", "URL", "Price", "Image Info", "Content HTML", "Source"])

    # Extract product information for the first website (e-dructer)
    product_info1 = extract_product_info(url1, base_url1, config1)
    all_products = pd.concat([all_products, product_info1], ignore_index=True)
    # Append the extracted product information to the all_products DataFrame

    # Extract product information for the second website (cosmomarket)
    product_info2 = extract_product_info(url2, base_url2, config2)
    all_products = pd.concat([all_products, product_info2], ignore_index=True)
    # Append the extracted product information to the all_products DataFrame

    # Extract product information for the third website (cookshop)
    product_info3 = extract_product_info(url3, base_url3, config3)
    all_products = pd.concat([all_products, product_info3], ignore_index=True)
    # Append the extracted product information to the all_products DataFrame

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
    # Store the combined DataFrame in the specified MySQL table.
