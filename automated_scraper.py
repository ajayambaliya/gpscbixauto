#!/usr/bin/env python3
"""
Automated scraper that runs on GitHub Actions to scrape current month's affairs
and create practice sets.
"""

import os
import sys
from datetime import datetime, date, timedelta
import time
import random
from dotenv import load_dotenv

# Load local .env file if running locally
load_dotenv()

def retry_with_backoff(func, max_retries=3, initial_delay=2):
    """
    Retry a function with exponential backoff
    
    Args:
        func: Function to retry
        max_retries: Maximum number of retries
        initial_delay: Initial delay in seconds
        
    Returns:
        Result of the function or None if failed
    """
    retry_count = 0
    while retry_count < max_retries:
        try:
            return func()
        except Exception as e:
            retry_count += 1
            if retry_count >= max_retries:
                print(f"❌ Maximum retries reached. Giving up.")
                return None
                
            delay = initial_delay * (2 ** (retry_count - 1)) * (0.5 + random.random())
            print(f"⚠️ Retry attempt {retry_count}/{max_retries} after {delay:.2f} seconds...")
            time.sleep(delay)

def main():
    """Main function for automated scraping"""
    
    print("=" * 50)
    print("AUTOMATED SCRAPER - RUNNING ON GITHUB ACTIONS")
    print(f"Current date and time: {datetime.now()}")
    print("=" * 50)
    
    # Import our custom modules
    try:
        from custom_scraper import generate_urls_for_month
        from process_url_wrapper import process_urls_safely
        from db_utils import get_connection, close_connections, is_url_already_scraped
        from practice_sets import create_practice_set, add_questions_to_practice_set
        from practice_set_creator import create_practice_set_for_month
    except ImportError as e:
        print(f"Error importing required modules: {e}")
        sys.exit(1)
    
    # Get current month and year
    current_date = date.today()
    year = current_date.year
    month = current_date.month
    
    # Yesterday's date (to check for new content)
    yesterday = current_date - timedelta(days=1)
    yesterday_formatted = yesterday.strftime("%Y-%m-%d")
    print(f"Yesterday's date: {yesterday_formatted}")
    
    print(f"\nChecking for new content for {month}/{year}")
    
    # Generate URLs for current month
    try:
        all_urls = generate_urls_for_month(year, month)
        print(f"Generated {len(all_urls)} URLs for {month}/{year}")
    except Exception as e:
        print(f"Error generating URLs: {e}")
        sys.exit(1)
        
    # Make a backup URL list if the main list is empty (rare case)
    if not all_urls:
        print("⚠️ No URLs generated for current month. Trying alternative approach...")
        try:
            # Try to generate a URL for yesterday
            yesterday_url = f"https://www.indiabix.com/current-affairs/{yesterday_formatted}"
            all_urls = [yesterday_url]
            print(f"Adding yesterday's URL: {yesterday_url}")
        except Exception as e:
            print(f"⚠️ Error generating alternative URL: {e}")
    
    new_urls = []
    
    # Check which URLs haven't been scraped yet
    try:
        for url in all_urls:
            # Only scrape new URLs
            if not is_url_already_scraped(url):
                # Make sure URL doesn't end with a trailing slash
                if url.endswith('/'):
                    url = url[:-1]
                new_urls.append(url)
    except Exception as e:
        print(f"Error checking URLs: {e}")
        sys.exit(1)
    
    print(f"Found {len(all_urls)} total URLs for current month")
    print(f"Found {len(new_urls)} new URLs to scrape")
    
    if not new_urls:
        print("No new URLs to scrape. Exiting.")
        sys.exit(0)
    
    # Establish database connection
    mysql_conn = None
    try:
        print("Establishing database connection...")
        mysql_conn = retry_with_backoff(get_connection)
        
        if not mysql_conn:
            print("Failed to establish database connection. Aborting.")
            sys.exit(1)
            
        # Process URLs using the safer method
        print(f"Processing {len(new_urls)} URLs...")
        start_time = time.time()
        success_count = process_urls_safely(new_urls, mysql_conn)
        end_time = time.time()
        
        elapsed_time = end_time - start_time
        minutes, seconds = divmod(elapsed_time, 60)
        
        print(f"\nSuccessfully processed {success_count} out of {len(new_urls)} URLs")
        print(f"Elapsed time: {int(minutes)} minutes and {seconds:.2f} seconds")
        
        # Create practice set for the month if we scraped new content
        if success_count > 0:
            print("\nCreating practice set for the month...")
            try:
                def create_practice_set_wrapper():
                    return create_practice_set_for_month(year, month)
                
                result = retry_with_backoff(create_practice_set_wrapper)
                if result:
                    print("Practice set created successfully!")
                else:
                    print("Failed to create practice set")
            except Exception as e:
                print(f"Error creating practice set: {e}")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        # Close connections
        if mysql_conn:
            try:
                close_connections(mysql_conn)
                print("Database connections closed")
            except Exception as e:
                print(f"Error closing connections: {e}")
    
    print("\nAutomated scraping completed!")
    

if __name__ == "__main__":
    main() 
