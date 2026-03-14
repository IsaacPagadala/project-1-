import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import json
from google.cloud import bigquery

all_books = []

# Scrape 50 pages
for page in range(1, 51):
    print(f"Scraping page {page}...")

    url = f"https://books.toscrape.com/catalogue/category/books_1/page-{page}.html"
    response = requests.get(url)

    soup = BeautifulSoup(response.text, "html.parser")

    books = soup.find_all("article", class_="product_pod")

    for book in books:
        title = book.h3.a["title"]

        price = book.find("p", class_="price_color").text.strip("Â£")

        stock = book.find("p", class_="instock availability").text.strip()

        all_books.append({
            "Title": title,
            "Price": price,
            "Stock": stock
        })

    time.sleep(0.5)

# Create dataframe
df = pd.DataFrame(all_books)

# Save raw data
df.to_csv("daily_prices_pagination.csv", index=False)

print("Dataset created with", len(df), "books")

# Data Cleaning
df["Price"] = pd.to_numeric(df["Price"])
 
df['Stock']=df['Stock'].str.replace('In stock(','').str.replace('available)','')

df["Currency"] = "GBP"

avg_price = df["Price"].mean()
max_price = df["Price"].max()

print("Work done!")
print(f"Average price: £{avg_price:.2f}")
print(f"Max price: £{max_price:.2f}")

# Save cleaned dataset
df.to_csv("cleaned_data.csv", index=False)

# --- FIXED BIGQUERY INTEGRATION ---

# 1. Authenticate using the Secret Key from GitHub
if "GCP_SA_KEY" in os.environ:
    # This part runs on GitHub Actions
    info = json.loads(os.environ.get("GCP_SA_KEY"))
    credentials = bigquery.Client.from_service_account_info(info)
    
    # Define your Table ID and Project ID
    # MAKE SURE TO REPLACE 'your-project-id' WITH YOUR ACTUAL PROJECT ID
    project_id = "your-project-id" 
    table_id = f"{project_id}.ecommerce_data.daily_book_prices"

    # 2. Push to BigQuery
    try:
        df.to_gbq(table_id, project_id=project_id, if_exists="append", credentials=credentials)
        print("Successfully uploaded 1000 rows to BigQuery!")
    except Exception as e:
        print(f"Error uploading to BigQuery: {e}")
else:
    # This runs on your laptop - it will just skip BigQuery so it doesn't crash
    print("Running locally: Skipping BigQuery upload because GCP_SA_KEY not found.")
