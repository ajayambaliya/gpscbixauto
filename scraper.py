import requests
import re
from bs4 import BeautifulSoup
from deep_translator import GoogleTranslator
import time
from datetime import datetime, timedelta
import random
import urllib3

# Suppress insecure request warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
    Extract date from the URL
    
    Args:
        url (str): URL to extract date from
        
    Returns:
        tuple: (formatted_date, db_date_format)
    """
    date_match = re.search(r'(\d{4}-\d{2}-\d{2})', url)
    if date_match:
        extracted_date = datetime.strptime(date_match.group(0), '%Y-%m-%d')
        return extracted_date.strftime('%d %B %Y'), extracted_date.strftime('%Y-%m-%d')
    return datetime.today().strftime('%d %B %Y'), datetime.today().strftime('%Y-%m-%d')

def extract_month_year_from_url(url):
    """
    Extract month and year from URL
    
    Args:
        url (str): URL to extract month and year from
        
    Returns:
        str: Month and year
    """
    date_match = re.search(r'(\d{4}-\d{2})-\d{2}', url)
    if date_match:
        month_year_str = date_match.group(1)
        extracted_date = datetime.strptime(month_year_str, '%Y-%m')
        return extracted_date.strftime('%B %Y')
    return datetime.today().strftime('%B %Y')

def scrape_current_affairs_content(url):
    """
    Scrape current affairs content from IndiaBix
    
    Args:
        url (str): URL to scrape
        
    Returns:
        list: List of questions
    """
    try:
        print(f"🔍 Scraping content from: {url}")
        
        # Add a random User-Agent to mimic browser behavior
        headers = {
            'User-Agent': f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(80, 110)}.0.{random.randint(1000, 9999)}.{random.randint(10, 999)} Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
        
        response = requests.get(url, headers=headers, verify=False)
        
        # Check if the response is valid
        if response.status_code != 200:
            print(f"⚠️ Failed to retrieve content from {url}, status code: {response.status_code}")
            return None
            
        soup = BeautifulSoup(response.text, 'html.parser')
        question_containers = soup.find_all('div', class_='bix-div-container')
        
        if not question_containers:
            print(f"⚠️ No content found for {url}, skipping...")
            return None
            
        questions = []
        
        for container in question_containers:
            try:
                # Extract question text
                question_text_div = container.find('div', class_='bix-td-qtxt')
                question_text = question_text_div.text.strip() if question_text_div else "No question text"
                
                # Extract correct answer key and options
                correct_answer_key = container.find('input', {'class': 'jq-hdnakq'}).get('value', '').strip()
                options = container.find_all('div', class_='bix-td-option-val')
                option_map = {chr(65 + idx): option.text.strip() for idx, option in enumerate(options)}
                
                # Get correct answer text
                correct_answer_text = option_map.get(correct_answer_key, "Unknown")
                
                # Extract explanation
                explanation_div = container.find('div', class_='bix-ans-description')
                explanation_text = explanation_div.text.strip() if explanation_div else "No explanation available"
                
                # Get index of correct answer (1-based for database format)
                correct_answer_index = ord(correct_answer_key) - ord('A') + 1 if correct_answer_key else 0
                
                questions.append({
                    'question_text': question_text,
                    'options': option_map,
                    'correct_answer_text': correct_answer_text,
                    'correct_answer_index': correct_answer_index,
                    'explanation': explanation_text,
                })
                
            except Exception as e:
                print(f"⚠️ Error processing question: {e}")
                
        return questions
        
    except Exception as e:
        print(f"❌ Error scraping content from {url}: {e}")
        return None

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
    Translate all question data to Gujarati
    
    Args:
        question_data (dict): Question data to translate
        
    Returns:
        tuple: (translated_question, translated_options, translated_explanation)
    """
    # Translate question
    translated_question = translate_to_gujarati(question_data['question_text'])
    
    # Translate options
    translated_options = []
    for _, option_text in sorted(question_data['options'].items()):
        translated_option = translate_to_gujarati(option_text)
        translated_options.append(translated_option)
    
    # Translate explanation
    translated_explanation = translate_to_gujarati(question_data['explanation'])
    
    return translated_question, translated_options, translated_explanation 