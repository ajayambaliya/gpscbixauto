#!/usr/bin/env python3
"""
URL processing wrapper for IndiaBix Current Affairs scraper.
This module provides improved error handling for URL processing.
"""

import sys
import time
import os
import random
from datetime import datetime, date
from dotenv import load_dotenv
import re

# Load environment variables
load_dotenv()

def process_url_safely(url, connection=None, max_retries=3):
    """
    Process a single URL with improved error handling
    
    Args:
        url (str): URL to process
        connection: MySQL connection
        max_retries: Maximum number of retries
        
    Returns:
        bool: True if successful, False otherwise
    """
    from db_utils import get_connection, close_connections
    from custom_scraper import process_url
    
    # Clean the URL to ensure it's properly formatted
    # Remove trailing slash and any colon that might be from error messages
    clean_url = url.strip()
    if clean_url.endswith('/'):
        clean_url = clean_url[:-1]
    if clean_url.endswith(':'):
        clean_url = clean_url[:-1]
    
    # Extract the date part to ensure a valid URL format
    date_match = re.search(r'(\d{4}-\d{2}-\d{2})', clean_url)
    if date_match:
        clean_url = f"https://www.indiabix.com/current-affairs/{date_match.group(1)}"
        if url != clean_url:
            print(f"🔧 Fixed URL format: {url} → {clean_url}")
            url = clean_url
    else:
        print(f"⚠️ Could not find a valid date in URL: {url}")
        return False
        
    # Verify URL format
    if not url.startswith('https://www.indiabix.com/current-affairs/'):
        print(f"❌ Invalid URL format: {url}")
        return False
        
    # Skip future dates - enhanced to be more precise
    try:
        date_part = url.split('/')[-1]
        url_date = datetime.strptime(date_part, '%Y-%m-%d').date()
        current_date = date.today()
        
        # More strict checking - even within the same month
        if url_date > current_date:
            print(f"⚠️ Skipping future date: {url_date} (today is {current_date})")
            return False
            
        # Accept 2025 as a valid year since we're using system date
        # No need to adjust dates anymore
    except Exception as e:
        print(f"⚠️ Error parsing date from URL: {str(e)}")
    
    # Output the date we're processing to help with debugging
    try:
        date_part = url.split('/')[-1]
        pretty_date = datetime.strptime(date_part, '%Y-%m-%d').strftime('%d %B %Y')
        print(f"📅 Processing: {pretty_date}")
    except:
        pass
    
    retry_count = 0
    connection_to_use = connection
    
    while retry_count < max_retries:
        try:
            # Create a new connection if needed
            if connection_to_use is None:
                connection_to_use = get_connection()
                if connection_to_use is None:
                    print(f"❌ Failed to establish database connection, retrying...")
                    retry_count += 1
                    if retry_count >= max_retries:
                        return False
                    time.sleep(2 * (2 ** retry_count) * (0.5 + random.random()))
                    continue
            
            # Process the URL
            result = process_url(url, connection_to_use)
            return result
            
        except Exception as e:
            print(f"❌ Error processing URL {url}: {str(e)}")
            retry_count += 1
            
            # Close the connection if there was an error
            if connection_to_use and connection_to_use != connection:
                try:
                    close_connections(connection_to_use)
                except:
                    pass
                connection_to_use = None
                
            if retry_count >= max_retries:
                print(f"❌ Maximum retries reached for URL: {url}")
                return False
                
            # Exponential backoff with jitter
            delay = 2 * (2 ** retry_count) * (0.5 + random.random())
            print(f"⚠️ Retry attempt {retry_count}/{max_retries} after {delay:.2f} seconds...")
            time.sleep(delay)
    
    return False

def process_urls_safely(urls, connection=None, max_workers=None):
    """
    Process multiple URLs safely with parallel execution and improved error handling
    
    Args:
        urls (list): List of URLs to process
        connection: MySQL connection (optional)
        max_workers: Maximum number of worker threads (optional)
        
    Returns:
        int: Number of successfully processed URLs
    """
    import concurrent.futures
    from tqdm import tqdm
    from db_utils import get_connection, mark_url_as_processed
    
    if not urls:
        print("No URLs to process")
        return 0
        
    # Use provided connection or create a new one
    conn = connection
    if not conn:
        conn = get_connection()
        if not conn:
            print("❌ Failed to establish MySQL connection")
            return 0
    
    success_count = 0
    total_urls = len(urls)
    
    # Use environment variable for worker count if not specified
    if max_workers is None:
        max_workers = int(os.getenv("MAX_WORKER_THREADS", 4))
        
    print(f"🔄 Processing {total_urls} URLs in parallel with {max_workers} workers")
    
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit each URL for processing with the safe wrapper
            future_to_url = {
                executor.submit(process_url_safely, url, conn): url for url in urls
            }
            
            # Create a progress bar
            with tqdm(total=total_urls, desc="Processing URLs", unit="url") as progress:
                for future in concurrent.futures.as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        result = future.result()
                        if result:
                            success_count += 1
                            # Mark URL as processed if successful
                            mark_url_as_processed(url)
                    except Exception as e:
                        print(f"❌ Error processing URL {url}: {str(e)}")
                    finally:
                        progress.update(1)
    
    except Exception as e:
        print(f"❌ Error during parallel processing: {str(e)}")
    
    print(f"✅ Successfully processed {success_count}/{total_urls} URLs")
    return success_count

if __name__ == "__main__":
    # Example usage
    if len(sys.argv) < 2:
        print("Usage: python process_url_wrapper.py <url>")
        sys.exit(1)
        
    url = sys.argv[1]
    success = process_url_safely(url)
    print(f"Processing result: {'Success' if success else 'Failed'}")
    sys.exit(0 if success else 1) 
