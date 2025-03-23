#!/usr/bin/env python3
"""
Web scraping module for IndiaBix Current Affairs scraper.
This module handles the web scraping and content extraction.
"""

import requests
import re
from bs4 import BeautifulSoup
from deep_translator import GoogleTranslator
import time
from datetime import datetime, timedelta
import random
import urllib3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Suppress insecure request warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# User agent for requests
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:108.0) Gecko/20100101 Firefox/108.0'
]

def get_urls_to_scrape(processed_urls=None):
    """
    Get URLs to scrape from the current month
    
    Args:
        processed_urls (list): List of already processed URLs
        
    Returns:
        list: List of URLs to scrape
    """
    today = datetime.today()
    first_day_of_month = today.replace(day=1)
    urls = []
    
    for i in range((today - first_day_of_month).days + 1):
        date = first_day_of_month + timedelta(days=i)
        formatted_date = date.strftime('%Y-%m-%d')
        url = f"https://www.indiabix.com/current-affairs/{formatted_date}/"
        
        # Skip if URL has already been processed
        if processed_urls and url in processed_urls:
            continue
            
        urls.append(url)
        
    return urls

def extract_date_from_url(url):
    """
    Extract date from URL
    
    Args:
        url (str): URL containing date
        
    Returns:
        tuple: (formatted_date, database_date)
    """
    try:
        match = re.search(r'current-affairs/(\d{4})-(\d{2})-(\d{2})', url)
        if match:
            year, month, day = match.groups()
            # Create datetime object
            date_obj = datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d")
            # Format for display
            formatted_date = date_obj.strftime("%d %B %Y")
            # Format for database
            database_date = date_obj.strftime("%Y-%m-%d")
            return formatted_date, database_date
    except Exception as e:
        print(f"Error extracting date from URL {url}: {str(e)}")
    
    return None, None

def extract_month_year_from_url(url):
    """
    Extract month and year from URL
    
    Args:
        url (str): URL containing date
        
    Returns:
        str: Month and year (e.g., "January 2023")
    """
    try:
        match = re.search(r'current-affairs/(\d{4})-(\d{2})', url)
        if match:
            year, month = match.groups()
            # Create datetime object
            date_obj = datetime.strptime(f"{year}-{month}-01", "%Y-%m-%d")
            # Format for display
            return date_obj.strftime("%B %Y")
    except Exception as e:
        print(f"Error extracting month/year from URL {url}: {str(e)}")
    
    return None

def scrape_current_affairs_content(url):
    """
    Scrape current affairs questions from a URL
    
    Args:
        url (str): URL to scrape
        
    Returns:
        list: List of question data
    """
    questions_data = []
    
    try:
        # Ensure URL doesn't end with a slash
        if url.endswith('/'):
            url = url[:-1]
            
        # Random delay to avoid rate limiting
        time.sleep(random.uniform(1, 3))
        
        # Select a random user agent
        headers = {'User-Agent': random.choice(USER_AGENTS)}
        
        # Make the request with proper headers and timeout
        response = requests.get(url, headers=headers, timeout=30, verify=False)
        
        # Check response status
        if response.status_code != 200:
            print(f"Failed to fetch URL: {url}, Status: {response.status_code}")
            return questions_data
        
        # Parse the content
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find question divs
        question_divs = soup.select('.bix-div-container')
        
        if not question_divs:
            print(f"No questions found at URL: {url}")
            return questions_data
        
        # Process each question
        for div in question_divs:
            try:
                # Extract question text
                question_elem = div.select_one('.bix-td-qtxt')
                if not question_elem:
                    continue
                    
                question_text = question_elem.get_text(strip=True)
                
                # Extract options
                options = []
                option_elems = div.select('.bix-td-option')
                for option_elem in option_elems:
                    option_text = option_elem.get_text(strip=True)
                    options.append(option_text)
                
                # Extract answer and explanation
                answer_text = ""
                explanation = ""
                
                # Find the correct answer index (0-based)
                correct_option_index = -1
                answer_div = div.select_one('.jq-hdnakqb')
                if answer_div:
                    answer_value = answer_div.get('value', '')
                    # Map answer value to index (a=0, b=1, c=2, d=3)
                    answer_map = {'a': 0, 'b': 1, 'c': 2, 'd': 3, 'e': 4}
                    correct_option_index = answer_map.get(answer_value.lower(), -1)
                
                # Extract explanation
                explanation_elem = div.select_one('.bix-ans-description')
                if explanation_elem:
                    explanation = explanation_elem.get_text(strip=True)
                
                # Skip if we don't have all required data
                if not question_text or not options or correct_option_index == -1:
                    continue
                
                # Store the question data
                question_data = {
                    'question': question_text,
                    'options': options,
                    'correct_option_index': correct_option_index,
                    'explanation': explanation,
                    'source_url': url
                }
                
                questions_data.append(question_data)
            except Exception as e:
                print(f"Error processing question: {str(e)}")
                continue
        
        print(f"Scraped {len(questions_data)} questions from {url}")
        
    except requests.exceptions.RequestException as e:
        print(f"Request error for URL {url}: {str(e)}")
    except Exception as e:
        error_type = type(e).__name__
        error_message = str(e)
        print(f"Error scraping URL {url}: {error_type}: {error_message}")
        
        # Handle specific errors
        if "bytearray index out of range" in error_message:
            print("ℹ️ This may be a website response issue. The page might not exist or might be formatted differently.")
            # Try a simpler parsing approach as fallback
            try:
                simple_response = requests.get(url, headers=headers, timeout=30, verify=False)
                if simple_response.status_code == 200:
                    simple_soup = BeautifulSoup(simple_response.text, 'html.parser')
                    title = simple_soup.title.string if simple_soup.title else "No title found"
                    print(f"Page title: {title}")
                    if "404" in title or "not found" in title.lower():
                        print("ℹ️ This appears to be a 404 page - the content doesn't exist.")
                    elif "current affairs" not in title.lower():
                        print("ℹ️ This doesn't appear to be a Current Affairs page.")
            except:
                pass
    
    return questions_data

def translate_to_gujarati(text, retries=3, delay=5):
    """
    Translate text to Gujarati with retry mechanism
    
    Args:
        text (str): Text to translate
        retries (int): Number of retries
        delay (int): Delay between retries
        
    Returns:
        str: Translated text
    """
    attempt = 0
    while attempt < retries:
        try:
            # Add a small delay to avoid rate limiting
            time.sleep(random.uniform(0.5, 1.5))
            
            # Translate the text
            translated = GoogleTranslator(source='auto', target='gu').translate(text)
            
            # If translation is successful, return the result
            if translated and translated.strip():
                return translated
                
            # If translation is empty but no exception occurred, retry
            print(f"⚠️ Empty translation result on attempt {attempt + 1}/{retries}")
            
        except Exception as e:
            attempt += 1
            print(f"⚠️ Translation attempt {attempt}/{retries} failed: {e}")
            
        # Wait before retrying
        if attempt < retries:
            actual_delay = delay * (attempt + 1)  # Increase delay with each retry
            print(f"⏳ Retrying in {actual_delay} seconds...")
            time.sleep(actual_delay)
            
    print("❌ Translation failed after multiple attempts. Returning original text.")
    return text

def translate_question_data(question_data):
    """
    Translate question data to Gujarati
    
    Args:
        question_data (dict): Question data to translate
        
    Returns:
        tuple: (translated_question, translated_options, translated_explanation)
    """
    try:
        # Initialize translator
        translator = GoogleTranslator(source='auto', target='gujarati')
        
        # Translate question
        translated_question = translator.translate(question_data['question'])
        
        # Translate options
        translated_options = []
        for option in question_data['options']:
            translated_option = translator.translate(option)
            translated_options.append(translated_option)
        
        # Translate explanation
        translated_explanation = translator.translate(question_data['explanation']) if question_data['explanation'] else ""
        
        return translated_question, translated_options, translated_explanation
        
    except Exception as e:
        print(f"Error during translation: {str(e)}")
        time.sleep(2)  # Wait before retry or return
        return None

if __name__ == "__main__":
    # Example usage
    import sys
    if len(sys.argv) > 1:
        test_url = sys.argv[1]
        print(f"Testing scraper with URL: {test_url}")
        questions = scrape_current_affairs_content(test_url)
        print(f"Found {len(questions)} questions")
        for i, q in enumerate(questions, 1):
            print(f"\nQuestion {i}:")
            print(f"Text: {q['question']}")
            for j, option in enumerate(q['options']):
                print(f"Option {j+1}: {option}")
            print(f"Correct: Option {q['correct_option_index']+1}")
            print(f"Explanation: {q['explanation']}")
    else:
        print("Please provide a URL to test")
        sys.exit(1) 
