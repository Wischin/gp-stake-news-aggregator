import feedparser
import pandas as pd
import datetime
import os

# List of RSS feeds from financial news outlets
# IMPORTANT: Always verify these URLs are current and working.
# Some sites may have paywalls or change feed structures, which can affect results.
# You can add more relevant RSS feed URLs here.
rss_feeds = {
    'Alternatives Watch': 'https://www.alternativeswatch.com/feed/',
    'Pensions & Investments': 'https://www.pionline.com/news/rss.xml', # Updated to a more general news feed URL
    'Financier Worldwide': 'https://www.financierworldwide.com/rss.html',
    # 'Institutional Investor - Alternative Investments': 'https://www.institutionalinvestor.com/feeds/alternative-investments', # May require subscription access
    # 'Preqin Insights (General)': 'https://www.preqin.com/news-insights/rss.xml', # Some articles may be behind a paywall
}

# Keywords to search for in article titles and summaries (case-insensitive)
keywords = [
    'GP stake', 'general partner stake', 'minority stake', 'majority stake',
    'alternative asset manager', 'asset manager investment',
    'private equity stake', 'fund manager stake', 'anchor investor'
]

def check_for_keywords(text, keywords):
    """Checks if any of the keywords are present in the given text."""
    if not text:
        return False
    # Check for whole words to avoid matching parts of words (e.g., "staking" vs "stake")
    return any(f" {keyword.lower()} " in f" {text.lower()} " or 
               text.lower().startswith(keyword.lower() + " ") or 
               text.lower().endswith(" " + keyword.lower()) or 
               text.lower() == keyword.lower()
               for keyword in keywords)

def get_gp_stake_news():
    """Aggregates news from RSS feeds and filters by keywords."""
    all_articles = []
    
    for source, url in rss_feeds.items():
        print(f"Fetching feed from: {source} ({url})")
        try:
            feed = feedparser.parse(url)
            if feed.bozo:
                print(f"Warning: Feed for {source} has parsing errors: {feed.bozo_exception}")

            for entry in feed.entries:
                title = entry.get('title', '')
                summary = entry.get('summary', '')
                link = entry.get('link', '')
                published_date = entry.get('published', 'N/A')
                
                # Check if keywords are in the title or summary
                if check_for_keywords(title, keywords) or check_for_keywords(summary, keywords):
                    all_articles.append({
                        'Source': source,
                        'Title': title,
                        'Link': link,
                        'Published': published_date,
                        'Summary': summary
                    })
        except Exception as e:
            print(f"Error fetching or parsing feed from {source}: {e}")
            continue # Continue to the next feed even if one fails

    return pd.DataFrame(all_articles)

if __name__ == '__main__':
    print("Starting GP Stake News Aggregator...")
    df = get_gp_stake_news()
    
    if not df.empty:
        # Generate a timestamped filename
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_name = f'gp_stake_news_{timestamp}.csv'
        
        # Ensure the 'data' directory exists
        output_dir = 'data'
        os.makedirs(output_dir, exist_ok=True)
        full_path = os.path.join(output_dir, file_name)

        df.to_csv(full_path, index=False, encoding='utf-8')
        print(f"GP stake news saved to {full_path}")
        
        # Display the results (optional, for local testing/debugging)
        # print("\n--- Latest GP Stake News ---")
        # print(df.head()) # Only show a few rows for brevity

        # You might also want to maintain a single 'latest.csv' file
        latest_file_name = os.path.join(output_dir, 'gp_stake_news_latest.csv')
        df.to_csv(latest_file_name, index=False, encoding='utf-8')
        print(f"Latest GP stake news also saved to {latest_file_name}")

    else:
        print("No new GP stake news found based on the provided keywords.")
    print("Script finished.")
