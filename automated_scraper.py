#!/usr/bin/env python3
"""
Automated scraper that runs on GitHub Actions to scrape current month's affairs
and create practice sets.
"""

import os
import sys
from datetime import datetime, date, timedelta
import time
from dotenv import load_dotenv

# Load local .env file if running locally
load_dotenv()

# Import our custom modules
try:
    from custom_scraper import generate_urls_for_month, process_urls_parallel
    from db_utils import get_connection, close_connections, is_url_already_scraped, mark_url_as_processed
    from practice_set_creator import create_practice_set_for_month
except ImportError as e:
    print(f"Error importing required modules: {e}")
    sys.exit(1)


def main():
    """Main function for automated scraping"""
    
    print("=" * 50)
    print("AUTOMATED SCRAPER - RUNNING ON GITHUB ACTIONS")
    print(f"Current date and time: {datetime.now()}")
    print("=" * 50)
    
    # Get current month and year
    current_date = date.today()
    year = current_date.year
    month = current_date.month
    
    # Yesterday's date (to check for new content)
    yesterday = current_date - timedelta(days=1)
    
    print(f"\nChecking for new content for {month}/{year}")
    
    # Generate URLs for current month
    all_urls = generate_urls_for_month(year, month)
    new_urls = []
    
    # Check which URLs haven't been scraped yet
    for url in all_urls:
        # Only scrape new URLs
        if not is_url_already_scraped(url):
            new_urls.append(url)
    
    print(f"Found {len(all_urls)} total URLs for current month")
    print(f"Found {len(new_urls)} new URLs to scrape")
    
    if not new_urls:
        print("No new URLs to scrape. Exiting.")
        sys.exit(0)
    
    # Establish database connection
    mysql_conn = None
    try:
        print("Establishing database connection...")
        mysql_conn = get_connection()
        
        if not mysql_conn:
            print("Failed to establish database connection. Aborting.")
            sys.exit(1)
            
        # Process URLs
        print(f"Processing {len(new_urls)} URLs...")
        start_time = time.time()
        success_count = process_urls_parallel(new_urls, mysql_conn)
        end_time = time.time()
        
        elapsed_time = end_time - start_time
        minutes, seconds = divmod(elapsed_time, 60)
        
        print(f"\nSuccessfully processed {success_count} out of {len(new_urls)} URLs")
        print(f"Elapsed time: {int(minutes)} minutes and {seconds:.2f} seconds")
        
        # Create practice set for the month if we scraped new content
        if success_count > 0:
            print("\nCreating practice set for the month...")
            result = create_practice_set_for_month(year, month)
            if result:
                print("Practice set created successfully!")
            else:
                print("Failed to create practice set")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        # Close connections
        if mysql_conn:
            close_connections(mysql_conn)
    
    print("\nAutomated scraping completed!")
    

if __name__ == "__main__":
    main()
