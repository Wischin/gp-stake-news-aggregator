import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import re

def extract_deal_info(article_text):
    """
    Extracts deal terms, transaction date, and parties involved from article text.
    This is a highly simplified example; real-world extraction might require
    more sophisticated NLP techniques or regex patterns specific to your sources.
    """
    deal_terms = re.findall(r'\$(\d+\.?\d*\s(?:million|billion|M|B))', article_text, re.IGNORECASE)
    transaction_date = re.search(r'\b(?:on|in)\s((?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{1,2},\s\d{4})', article_text)
    parties_involved = re.findall(r'\b(?:acquired|invested in|sold to)\s([A-Za-z0-9\s,&-]+(?:Inc|Corp|LLC|Ltd|Group|Holdings))', article_text)

    return {
        'deal_terms': ', '.join(deal_terms) if deal_terms else 'N/A',
        'transaction_date': transaction_date.group(1) if transaction_date else 'N/A',
        'parties_involved': ', '.join(parties_involved) if parties_involved else 'N/A'
    }

def scrape_news_source(url, last_year_date):
    """
    Scrapes a single news source for relevant articles.
    You will need to customize this for each specific news website.
    """
    articles_data = []
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status() # Raise an exception for bad status codes
        soup = BeautifulSoup(response.content, 'html.parser')

        # --- CUSTOMIZE THIS SECTION FOR EACH WEBSITE ---
        # Find elements containing news articles, e.g., div with a specific class
        news_elements = soup.find_all('article', class_='gp-stake-news-item') # Example class

        for article_element in news_elements:
            title_element = article_element.find('h2', class_='article-title')
            link_element = article_element.find('a', class_='article-link')
            date_element = article_element.find('time', class_='article-date')
            content_element = article_element.find('div', class_='article-content')

            if not all([title_element, link_element, date_element, content_element]):
                continue

            title = title_element.get_text(strip=True)
            link = link_element['href']
            publish_date_str = date_element.get_text(strip=True)
            article_content = content_element.get_text(strip=True)

            try:
                # Parse date - adjust format as per website's date format
                publish_date = datetime.strptime(publish_date_str, '%B %d, %Y') # Example format
            except ValueError:
                print(f"Could not parse date: {publish_date_str}")
                continue

            # Filter for articles within the last year
            if publish_date >= last_year_date:
                deal_info = extract_deal_info(article_content)
                articles_data.append({
                    'Title': title,
                    'Link': link,
                    'Published Date': publish_date.strftime('%Y-%m-%d'),
                    'Deal Terms': deal_info['deal_terms'],
                    'Transaction Date': deal_info['transaction_date'],
                    'Parties Involved': deal_info['parties_involved'],
                })
        # --- END CUSTOMIZATION SECTION ---

    except requests.exceptions.RequestException as e:
        print(f"Error scraping {url}: {e}")
    return articles_data

if __name__ == "__main__":
    today = datetime.now()
    one_year_ago = today - timedelta(days=365) # Filter for the last year

    # List of GP stake news sources (replace with actual URLs)
    news_sources = [
        "https://example.com/gp-stake-news",
        "https://another-source.com/private-equity-deals",
        # Add more sources as needed
    ]

    all_aggregated_news = []
    for source_url in news_sources:
        print(f"Scraping {source_url}...")
        all_aggregated_news.extend(scrape_news_source(source_url, one_year_ago))

    if all_aggregated_news:
        df = pd.DataFrame(all_aggregated_news)
        df.to_csv('gp_stake_news_data.csv', index=False)
        print(f"Aggregated {len(df)} news articles and saved to gp_stake_news_data.csv")
    else:
        print("No news articles found or aggregated.")
