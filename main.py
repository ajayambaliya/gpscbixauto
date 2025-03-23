import requests
import mysql.connector
import re
import time
import pymongo
import json
import random
import string
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from deep_translator import GoogleTranslator
import os
from dotenv import load_dotenv

# Import modules
from db_utils import (
    create_mysql_connection, 
    get_or_create_skill, 
    get_or_create_topic, 
    insert_question, 
    mark_url_as_processed, 
    get_processed_urls,
    close_connections,
    get_connection,
    is_url_already_scraped,
    get_scraping_stats
)
from scraper import (
    get_urls_to_scrape, 
    extract_date_from_url, 
    extract_month_year_from_url, 
    scrape_current_affairs_content,
    translate_question_data
)

# Load environment variables
load_dotenv()

def main():
    """Main function to coordinate the scraping and database operations"""
    try:
        print("🚀 Starting Current Affairs Scraper")
        
        # Create database connection
        connection = get_connection()
        if not connection:
            print("❌ Aborting: Failed to establish MySQL connection")
            return
            
        # Get list of already processed URLs
        processed_urls = get_processed_urls()
        print(f"ℹ️ Found {len(processed_urls)} already processed URLs")
        
        # Get URLs to scrape
        all_urls = get_urls_to_scrape(processed_urls)
        
        if not all_urls:
            # Display scraping stats
            stats = get_scraping_stats()
            print("\n📊 Overall Scraping Statistics:")
            print(f"Total URLs scraped to date: {stats['total_urls_scraped']}")
            print("Monthly breakdown:")
            for month_stat in stats['monthly_breakdown'][:5]:  # Show top 5 months
                print(f"  - {month_stat['month']}: {month_stat['count']} URLs")
                
            print("✅ No new URLs to scrape. Exiting...")
            return
            
        print(f"📋 Found {len(all_urls)} new URLs to scrape")
        
        # Process each URL
        for index, url in enumerate(all_urls, 1):
            try:
                print(f"\n📌 Processing URL {index}/{len(all_urls)}: {url}")
                
                # Check if URL has already been scraped
                if is_url_already_scraped(url):
                    print(f"⏭️ URL already scraped, skipping: {url}")
                    continue
                
                # Extract date information
                formatted_date, news_date = extract_date_from_url(url)
                month_year = extract_month_year_from_url(url)
                topic_name = f"{formatted_date} Current Affairs"
                
                print(f"📅 Date: {formatted_date}, Month-Year: {month_year}")
                
                # Check if connection is still active and reconnect if needed
                if not connection or not hasattr(connection, 'is_connected') or not connection.is_connected():
                    connection = get_connection()
                    if not connection:
                        print("❌ Failed to reconnect to MySQL")
                        continue
                
                # Get or create skill for the month-year
                skill_id = get_or_create_skill(connection, month_year)
                if not skill_id:
                    print(f"❌ Failed to get/create skill for {month_year}, skipping URL")
                    continue
                    
                # Get or create topic for the date
                topic_id = get_or_create_topic(connection, topic_name, skill_id)
                if not topic_id:
                    print(f"❌ Failed to get/create topic for {topic_name}, skipping URL")
                    continue
                    
                # Scrape content
                questions = scrape_current_affairs_content(url)
                if not questions:
                    print(f"❌ No questions found for {url}, skipping")
                    mark_url_as_processed(url)  # Mark as processed even if no questions found
                    continue
                    
                print(f"📝 Found {len(questions)} questions for {formatted_date}")
                
                # Insert each question into the database
                success_count = 0
                fail_count = 0
                
                for i, question_data in enumerate(questions, 1):
                    print(f"⏳ Processing question {i}/{len(questions)}")
                    
                    # Translate question data
                    translated_data = translate_question_data(question_data)
                    if not translated_data:
                        print(f"❌ Failed to translate question {i}/{len(questions)}")
                        fail_count += 1
                        continue
                    
                    translated_question, translated_options, translated_explanation = translated_data
                    
                    # Check if connection is still active and reconnect if needed
                    if not connection or not hasattr(connection, 'is_connected') or not connection.is_connected():
                        connection = get_connection()
                        if not connection:
                            print("❌ Failed to reconnect to MySQL")
                            fail_count += 1
                            continue
                    
                    # Insert question
                    question_id = insert_question(
                        connection, 
                        question_data, 
                        skill_id, 
                        topic_id,
                        translated_question,
                        translated_options,
                        translated_explanation
                    )
                    
                    if question_id:
                        print(f"✅ Successfully added question {i}/{len(questions)} with ID: {question_id}")
                        success_count += 1
                    else:
                        print(f"❌ Failed to add question {i}/{len(questions)}")
                        fail_count += 1
                    
                    # Add a small delay to avoid overwhelming resources
                    time.sleep(0.5)
                    
                # Mark URL as processed
                mark_url_as_processed(url)
                
                print(f"📊 Summary for {url}: {success_count} questions added, {fail_count} failed")
                
                # Add a delay between URLs
                if index < len(all_urls):
                    print(f"⏳ Waiting 2 seconds before processing next URL...")
                    time.sleep(2)
                
            except Exception as e:
                print(f"❌ Error processing URL {url}: {str(e)}")
                
        # Display overall stats
        stats = get_scraping_stats()
        print("\n📊 Overall Scraping Statistics:")
        print(f"Total URLs scraped to date: {stats['total_urls_scraped']}")
        print("Monthly breakdown:")
        for month_stat in stats['monthly_breakdown'][:5]:  # Show top 5 months
            print(f"  - {month_stat['month']}: {month_stat['count']} URLs")
                
    except Exception as e:
        print(f"❌ Error in main process: {str(e)}")
    finally:
        # Close connections
        close_connections(connection)
        print("\n✅ Scraping process completed")

if __name__ == "__main__":
    main()
