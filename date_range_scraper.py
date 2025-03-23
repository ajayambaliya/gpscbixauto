import sys
import time
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
import concurrent.futures
from tqdm import tqdm
import os

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

def generate_urls_for_date_range(start_date, end_date):
    """
    Generate URLs for all dates in a specific range
    
    Args:
        start_date (date): Start date
        end_date (date): End date
        
    Returns:
        list: List of URLs to scrape
    """
    urls = []
    current_date = start_date
    
    while current_date <= end_date:
        # Format the date as required by the URL
        formatted_date = current_date.strftime("%Y-%m-%d")
        url = f"https://www.indiabix.com/current-affairs/{formatted_date}/"
        urls.append(url)
        
        # Move to the next day
        current_date += timedelta(days=1)
    
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

def process_urls_parallel(urls_to_scrape, main_conn):
    """
    Process multiple URLs in parallel
    
    Args:
        urls_to_scrape (list): List of URLs to scrape
        main_conn: Main MySQL connection
        
    Returns:
        int: Number of successfully processed URLs
    """
    total_urls = len(urls_to_scrape)
    success_count = 0
    
    # For each URL, we need to get skill and topic info first
    # We can't parallelize this part due to database dependencies
    for i, url in enumerate(urls_to_scrape, 1):
        print(f"\n🔍 Processing URL {i}/{total_urls}: {url}")
        
        # Make sure we have a valid connection for each URL
        if main_conn is None or not hasattr(main_conn, 'is_connected') or not main_conn.is_connected():
            print("ℹ️ Connection not valid, getting a new one...")
            main_conn = get_connection()
            if not main_conn:
                print("❌ Failed to get a valid database connection, skipping URL...")
                continue
                
        # Process the URL with a valid connection
        success = process_url(url, main_conn)
        if success:
            success_count += 1
    
    return success_count

def main():
    """Main function"""
    # Check if arguments are provided
    if len(sys.argv) < 3:
        print("❌ Usage: python date_range_scraper.py <start_date> <end_date>")
        print("❌ Date format: YYYY-MM-DD")
        print("❌ Example: python date_range_scraper.py 2023-05-01 2023-05-15")
        sys.exit(1)
    
    # Parse arguments
    try:
        start_date_str = sys.argv[1]
        end_date_str = sys.argv[2]
        
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    except ValueError:
        print("❌ Invalid date format. Expected format: YYYY-MM-DD")
        sys.exit(1)
    
    # Validate date range
    if start_date > end_date:
        print("❌ Start date cannot be later than end date")
        sys.exit(1)
    
    current_date = date.today()
    if end_date > current_date:
        print("⚠️ Warning: End date is in the future. Adjusting to today's date.")
        end_date = current_date
    
    print("🚀 Starting Date Range Current Affairs Scraper")
    print(f"📆 Date Range: {start_date} to {end_date}")
    print(f"💡 Using {MAX_WORKERS} worker threads for parallel processing")
    
    # Generate URLs
    all_urls = generate_urls_for_date_range(start_date, end_date)
    
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

if __name__ == "__main__":
    main() 