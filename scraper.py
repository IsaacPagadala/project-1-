import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

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