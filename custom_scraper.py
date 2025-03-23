import sys
import time
import os
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
import concurrent.futures
from tqdm import tqdm

# Import modules
from db_utils import (
    create_mysql_connection, 
    get_or_create_skill, 
    get_or_create_topic, 
    insert_question, 
    mark_url_as_processed,
    close_connections,
    get_connection,  # Import the new connection function
    is_url_already_scraped,  # Import the new URL checking function
    get_scraping_stats  # Import the stats function
)
from scraper import (
    extract_date_from_url, 
    extract_month_year_from_url, 
    scrape_current_affairs_content,
    translate_question_data
)

# Load environment variables
load_dotenv()

# Number of worker threads to use for parallel processing
MAX_WORKERS = int(os.getenv("MAX_WORKER_THREADS", 4))

def generate_url(year, month, day=None):
    """
    Generate URL for scraping based on the provided date parameters
    
    Args:
        year (int): Year
        month (int): Month
        day (int, optional): Day. If None, returns all days in the month
        
    Returns:
        list: List of URLs to scrape
    """
    urls = []
    
    # If day is provided, generate URL for specific date
    if day:
        formatted_date = f"{year}-{month:02d}-{day:02d}"
        url = f"https://www.indiabix.com/current-affairs/{formatted_date}/"
        urls.append(url)
    else:
        # If day is not provided, generate URLs for all days in the month
        # Determine the number of days in the month
        if month == 2:
            # Check for leap year
            if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
                days_in_month = 29
            else:
                days_in_month = 28
        elif month in [4, 6, 9, 11]:
            days_in_month = 30
        else:
            days_in_month = 31
        
        # Generate URLs for each day in the month
        for day in range(1, days_in_month + 1):
            formatted_date = f"{year}-{month:02d}-{day:02d}"
            url = f"https://www.indiabix.com/current-affairs/{formatted_date}/"
            urls.append(url)
    
    return urls

def process_question(args):
    """
    Process a single question
    
    Args:
        args (tuple): Tuple containing (question_data, skill_id, topic_id, total_questions, index)
        
    Returns:
        tuple: (success, question_id)
    """
    question_data, skill_id, topic_id, total_questions, index = args
    conn = None
    max_retries = 3
    retry_count = 0
    
    try:
        # Translate question data to Gujarati
        translated_data = translate_question_data(question_data)
        if not translated_data:
            return (False, None)
        
        translated_question, translated_options, translated_explanation = translated_data
        
        # Database operation with retry logic
        while retry_count < max_retries:
            try:
                # Get a new database connection for this thread
                conn = get_connection()
                if not conn:
                    retry_count += 1
                    if retry_count >= max_retries:
                        return (False, None)
                    time.sleep(2 * retry_count)  # Exponential backoff
                    continue
                
                # Insert question into database
                question_id = insert_question(
                    conn, 
                    question_data, 
                    skill_id, 
                    topic_id, 
                    translated_question, 
                    translated_options, 
                    translated_explanation
                )
                
                if not question_id:
                    retry_count += 1
                    if retry_count >= max_retries:
                        return (False, None)
                    time.sleep(2 * retry_count)  # Exponential backoff
                    continue
                
                return (True, question_id)
                
            except Exception as db_err:
                retry_count += 1
                if retry_count >= max_retries:
                    return (False, None)
                time.sleep(2 * retry_count)  # Exponential backoff
            finally:
                # Always close the connection after each try
                if conn is not None:
                    try:
                        close_connections(conn)
                        conn = None
                    except:
                        pass
        
        return (False, None)
        
    except Exception as e:
        return (False, None)
    finally:
        # Ensure connection is closed if it hasn't been closed yet
        if conn is not None:
            try:
                close_connections(conn)
            except:
                pass

def process_url(url, conn, retry_count=0, max_retries=3):
    """
    Process a URL and extract current affairs questions
    
    Args:
        url (str): URL to scrape
        conn: MySQL connection
        retry_count (int): Current retry count
        max_retries (int): Maximum number of retries
        
    Returns:
        bool: True if processing succeeded, False otherwise
    """
    try:
        # Extract date from URL
        date_text, date_db = extract_date_from_url(url)
        month_year = extract_month_year_from_url(url)
        
        if not date_text or not month_year:
            print(f"❌ Failed to extract date or month from URL: {url}")
            return False
        
        print(f"📅 Date: {date_text}, Month-Year: {month_year}")
        
        # Check if connection is valid, get a new one if needed
        if conn is None or not hasattr(conn, 'is_connected') or not conn.is_connected():
            print("ℹ️ Connection not valid, getting a new one...")
            conn = get_connection()
            if not conn:
                print("❌ Failed to get a valid database connection")
                return False
                
        # Create or get skill and topic IDs
        skill_id = get_or_create_skill(conn, month_year)
        if not skill_id:
            print(f"❌ Failed to create or get skill for: {month_year}")
            # Try to reconnect and retry
            conn = get_connection()
            if not conn:
                print("❌ Failed to reconnect to database")
                return False
            skill_id = get_or_create_skill(conn, month_year)
            if not skill_id:
                print(f"❌ Failed to create or get skill after retry")
                return False
        
        topic_name = f"{date_text} Current Affairs"
        topic_id = get_or_create_topic(conn, topic_name, skill_id)
        if not topic_id:
            print(f"❌ Failed to create or get topic for: {topic_name}")
            # Try to reconnect and retry
            conn = get_connection()
            if not conn:
                print("❌ Failed to reconnect to database")
                return False
            topic_id = get_or_create_topic(conn, topic_name, skill_id)
            if not topic_id:
                print(f"❌ Failed to create or get topic after retry")
                return False
        
        # Scrape the content
        print(f"🔍 Scraping content from: {url}")
        questions_data = scrape_current_affairs_content(url)
        
        if not questions_data:
            print(f"❌ No questions found on: {url}")
            return False
        
        print(f"✅ Found {len(questions_data)} questions")
        
        # Process questions in parallel
        total_questions = len(questions_data)
        success_count = 0
        
        # Create a list of arguments for each question
        question_args = [
            (question_data, skill_id, topic_id, total_questions, i) 
            for i, question_data in enumerate(questions_data, 1)
        ]
        
        # Use ThreadPoolExecutor to process questions in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all tasks and get futures
            futures = {executor.submit(process_question, args): args for args in question_args}
            
            # Process results as they complete with a progress bar
            with tqdm(total=total_questions, desc="Processing questions", unit="question") as pbar:
                for future in concurrent.futures.as_completed(futures):
                    success, question_id = future.result()
                    if success:
                        success_count += 1
                        pbar.set_postfix({"Success": f"{success_count}/{total_questions}"})
                    pbar.update(1)
        
        print(f"✅ Successfully processed {success_count}/{total_questions} questions")
        
        # Mark URL as processed
        mark_url_as_processed(url)
        return success_count > 0
        
    except Exception as e:
        print(f"❌ Error processing URL {url}: {str(e)}")
        
        # Retry if not exceeded max retries
        if retry_count < max_retries:
            print(f"⚠️ Retrying ({retry_count + 1}/{max_retries})...")
            time.sleep(2 * (retry_count + 1))  # Exponential backoff
            # Get a fresh connection for the retry
            new_conn = get_connection()
            return process_url(url, new_conn, retry_count + 1, max_retries)
        
        return False

def process_urls_parallel(urls, connection=None):
    """
    Process a list of URLs in parallel
    
    Args:
        urls (list): List of URLs to process
        connection: MySQL connection (optional)
        
    Returns:
        int: Number of successfully processed URLs
    """
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
    
    print(f"🔄 Processing {total_urls} URLs in parallel with {MAX_WORKERS} workers")
    
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit each URL for processing
            future_to_url = {executor.submit(process_url, url, conn): url for url in urls}
            
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

def main():
    """Main function"""
    # Check if arguments are provided
    if len(sys.argv) < 3:
        print("❌ Usage: python custom_scraper.py <year> <month> [day]")
        sys.exit(1)
    
    # Parse arguments
    year = int(sys.argv[1])
    month = int(sys.argv[2])
    day = int(sys.argv[3]) if len(sys.argv) > 3 else None
    
    # Validate arguments
    current_date = datetime.now()
    if year > current_date.year or (year == current_date.year and month > current_date.month):
        print("❌ Cannot scrape future dates")
        sys.exit(1)
    
    if month < 1 or month > 12:
        print("❌ Month must be between 1 and 12")
        sys.exit(1)
    
    if day and (day < 1 or day > 31):
        print("❌ Day must be between 1 and 31")
        sys.exit(1)
    
    print("🚀 Starting Custom Current Affairs Scraper")
    print(f"💡 Using {MAX_WORKERS} worker threads for parallel processing")
    
    # Generate URLs
    all_urls = generate_url(year, month, day)
    
    # Filter out already scraped URLs
    urls_to_scrape = []
    skipped_urls = []
    
    for url in all_urls:
        if is_url_already_scraped(url):
            skipped_urls.append(url)
        else:
            urls_to_scrape.append(url)
    
    print(f"📋 Found {len(all_urls)} total URLs")
    print(f"⏭️ Skipping {len(skipped_urls)} already scraped URLs")
    print(f"🔍 Will scrape {len(urls_to_scrape)} new URLs")
    
    if not urls_to_scrape:
        print("✅ No new URLs to scrape. Exiting...")
        
        # Display scraping stats
        try:
            stats = get_scraping_stats()
            print("\n📊 Overall Scraping Statistics:")
            print(f"Total URLs scraped to date: {stats['total_urls_scraped']}")
            print("Monthly breakdown:")
            for month_stat in stats['monthly_breakdown'][:5]:  # Show top 5 months
                print(f"  - {month_stat['month']}: {month_stat['count']} URLs")
        except Exception as e:
            print(f"⚠️ Unable to retrieve scraping stats: {str(e)}")
        
        sys.exit(0)
    
    # Establish database connection
    mysql_conn = None
    success_count = 0
    
    try:
        # Establish initial database connection
        print("🔄 Establishing database connection...")
        mysql_conn = get_connection()
        
        if not mysql_conn:
            print("⚠️ Failed to establish initial database connection")
            print("Will attempt to reconnect during processing...")
        else:
            print("✅ Initial database connection established")
            
        # Process URLs (connection will be refreshed as needed)
        start_time = time.time()
        success_count = process_urls_parallel(urls_to_scrape, mysql_conn)
        end_time = time.time()
        
        # Calculate timing
        elapsed_time = end_time - start_time
        minutes, seconds = divmod(elapsed_time, 60)
        
    except Exception as e:
        print(f"❌ An error occurred during processing: {str(e)}")
    finally:
        # Close connections safely
        if mysql_conn is not None:
            try:
                close_connections(mysql_conn)
                print("✅ Database connection closed successfully")
            except Exception as e:
                print(f"⚠️ Warning when closing main connection: {str(e)}")
    
    # Print summary
    print("\n📊 Scraping Summary:")
    print(f"Total new URLs: {len(urls_to_scrape)}")
    print(f"Successfully processed: {success_count}")
    print(f"Failed: {len(urls_to_scrape) - success_count}")
    print(f"Skipped (already scraped): {len(skipped_urls)}")
    
    if 'elapsed_time' in locals():
        print(f"Total time: {int(minutes)} minutes and {seconds:.2f} seconds")
    
    # Display overall stats
    try:
        stats = get_scraping_stats()
        print("\n📊 Overall Scraping Statistics:")
        print(f"Total URLs scraped to date: {stats['total_urls_scraped']}")
    except Exception as e:
        print(f"⚠️ Could not retrieve scraping statistics: {str(e)}")
    
    print("\n✅ Scraping process completed")

def get_scraping_stats():
    """
    Get statistics about the scraped URLs from the database
    
    Returns:
        dict: A dictionary containing statistics
    """
    stats = {
        'total_urls_scraped': 0,
        'monthly_breakdown': []
    }
    
    conn = None
    try:
        conn = get_connection()
        if not conn:
            print("⚠️ Could not get connection for statistics")
            return stats
            
        cursor = conn.cursor(dictionary=True)
        
        # Get total count
        cursor.execute("""
            SELECT COUNT(DISTINCT source_url) as total
            FROM questions
            WHERE source_url LIKE 'https://www.indiabix.com/current-affairs/%'
        """)
        result = cursor.fetchone()
        stats['total_urls_scraped'] = result['total'] if result else 0
        
        # Get monthly breakdown
        cursor.execute("""
            SELECT 
                CONCAT(SUBSTRING_INDEX(SUBSTRING_INDEX(source_url, '/', -1), '-', 2)) as month_year,
                COUNT(DISTINCT source_url) as url_count
            FROM questions
            WHERE source_url LIKE 'https://www.indiabix.com/current-affairs/%'
            GROUP BY month_year
            ORDER BY url_count DESC
        """)
        
        monthly_results = cursor.fetchall()
        stats['monthly_breakdown'] = monthly_results
        
        cursor.close()
    except Exception as e:
        print(f"⚠️ Error getting scraping stats: {str(e)}")
    finally:
        if conn and conn.is_connected():
            try:
                conn.close()
            except Exception:
                pass
    
    return stats

def generate_urls_for_month(year, month):
    """
    Generate URLs for a specific month and year
    
    This function is a wrapper around generate_url to make it more accessible
    when importing from other scripts.
    
    Args:
        year (int): Year (e.g., 2024)
        month (int): Month (1-12)
        
    Returns:
        list: List of URLs for each day in the month
    """
    return generate_url(year, month)

if __name__ == "__main__":
    main() 
