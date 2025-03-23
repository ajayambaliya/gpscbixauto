import mysql.connector
import pymongo
import os
import random
import string
import json
from datetime import datetime
import re
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI")
mongo_client = pymongo.MongoClient(MONGO_URI)
db = mongo_client["CurrentAffairs"]
scraped_urls_collection = db["ScrapedURLs"]  # Collection for tracking scraped URLs
questions_collection = db["Questions"]

# MySQL Configuration
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

# Constants
SECTION_ID = 8  # Fixed section ID as per requirements
DIFFICULTY_LEVEL_ID = 1  # Fixed difficulty level ID as per requirements

# Global connection object for persistence
mysql_connection = None

def generate_random_code(prefix, length=10):
    """Generate a random code with a specific prefix"""
    characters = string.ascii_letters + string.digits
    random_string = ''.join(random.choice(characters) for _ in range(length))
    return f"{prefix}{random_string}"

def create_mysql_connection():
    """Create a new MySQL connection"""
    try:
        # Load environment variables
        mysql_host = os.getenv("MYSQL_HOST")
        mysql_user = os.getenv("MYSQL_USER")
        mysql_password = os.getenv("MYSQL_PASSWORD")
        mysql_database = os.getenv("MYSQL_DATABASE")
        
        # Get SSL verification setting from env (default to true for security)
        verify_ssl = os.getenv("MYSQL_VERIFY_SSL", "true").lower() == "true"
        
        # Connection parameters
        conn_params = {
            "host": mysql_host,
            "user": mysql_user,
            "password": mysql_password,
            "database": mysql_database,
            "connection_timeout": 30,  # Add timeout for connection attempts
            "use_pure": True,          # Use pure Python implementation
            "autocommit": False,       # We'll manually commit transactions
            "pool_size": 5,            # Connection pool size
            "pool_name": "scraper_pool",
            "pool_reset_session": True  # Reset session on connection return to pool
        }
        
        # Add SSL configuration if needed
        if not verify_ssl:
            # For mysql-connector-python 8.0.28 and higher:
            conn_params["ssl_disabled"] = True
            print("⚠️ SSL certificate verification disabled")
        
        # Create connection
        connection = mysql.connector.connect(**conn_params)
        
        print("✅ MySQL connection established successfully")
        
        # Create global connection if not exists
        global mysql_connection
        if mysql_connection is None:
            mysql_connection = connection
        
        return connection
        
    except mysql.connector.Error as err:
        print(f"❌ MySQL Connection Error: {err}")
        if mysql_connection:
            try:
                mysql_connection.close()
            except:
                pass
            
        mysql_connection = None
        return None

def get_connection():
    """Get an active MySQL connection, creating a new one if needed"""
    global mysql_connection
    
    # Check if there's an existing connection and it's active
    if mysql_connection is not None:
        try:
            # Test if connection is still alive
            if mysql_connection.is_connected():
                return mysql_connection
            else:
                # Connection was closed or lost, try to close it properly
                try:
                    mysql_connection.close()
                except:
                    pass
                mysql_connection = None
        except Exception as e:
            # Error checking connection, recreate it
            try:
                mysql_connection.close()
            except:
                pass
            mysql_connection = None
    
    # Try to create a new connection with retries
    retries = 3
    retry_delay = 2  # seconds
    
    for i in range(retries):
        try:
            # Create a new connection
            new_connection = create_mysql_connection()
            if new_connection:
                return new_connection
            
            # If we reach here, connection failed but didn't raise an exception
            print(f"⚠️ Connection attempt {i+1}/{retries} failed, retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
            retry_delay *= 2  # Exponential backoff
        except Exception as e:
            print(f"⚠️ Connection attempt {i+1}/{retries} failed with error: {str(e)}")
            if i < retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
    
    print("❌ All connection attempts failed")
    return None

def create_slug(text):
    """Create a slug from text"""
    # Convert to lowercase
    slug = text.lower()
    # Replace spaces with hyphens
    slug = slug.replace(' ', '-')
    # Remove special characters
    slug = re.sub(r'[^a-z0-9-]', '', slug)
    # Remove multiple hyphens
    slug = re.sub(r'-+', '-', slug)
    return slug

def get_or_create_skill(connection, month_year, section_id=SECTION_ID):
    """Get or create a skill based on month and year"""
    try:
        # Ensure we have a valid connection
        if not connection or not connection.is_connected():
            connection = get_connection()
            if not connection:
                print("❌ Cannot establish MySQL connection for skill creation")
                return None
        
        cursor = connection.cursor(dictionary=True)
        # Check if skill already exists
        query = "SELECT id FROM skills WHERE name = %s AND section_id = %s AND deleted_at IS NULL"
        cursor.execute(query, (month_year, section_id))
        result = cursor.fetchone()
        
        if result:
            skill_id = result['id']
            print(f"✅ Skill '{month_year}' already exists with ID: {skill_id}")
            cursor.close()
            return skill_id
        
        # Create new skill
        code = generate_random_code("skl_")
        slug = create_slug(month_year)
        
        query = """
        INSERT INTO skills (name, code, slug, section_id, short_description, is_active, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        current_time = datetime.now()
        data = (month_year, code, slug, section_id, f"Current Affairs for {month_year}", 1, current_time, current_time)
        
        cursor.execute(query, data)
        connection.commit()
        skill_id = cursor.lastrowid
        
        print(f"✅ Created new skill '{month_year}' with ID: {skill_id}")
        cursor.close()
        return skill_id
        
    except mysql.connector.Error as err:
        print(f"❌ Error in get_or_create_skill: {err}")
        # Try to reconnect on connection error
        if "MySQL Connection not available" in str(err) or "Not connected" in str(err):
            global mysql_connection
            mysql_connection = None
        return None

def get_or_create_topic(connection, date_text, skill_id):
    """Get or create a topic based on date and skill ID"""
    try:
        # Ensure we have a valid connection
        if not connection or not connection.is_connected():
            connection = get_connection()
            if not connection:
                print("❌ Cannot establish MySQL connection for topic creation")
                return None
        
        cursor = connection.cursor(dictionary=True)
        # Check if topic already exists
        query = "SELECT id FROM topics WHERE name = %s AND skill_id = %s AND deleted_at IS NULL"
        cursor.execute(query, (date_text, skill_id))
        result = cursor.fetchone()
        
        if result:
            topic_id = result['id']
            print(f"✅ Topic '{date_text}' already exists with ID: {topic_id}")
            cursor.close()
            return topic_id
        
        # Create new topic
        code = generate_random_code("top_")
        slug = create_slug(date_text)
        
        query = """
        INSERT INTO topics (name, code, slug, skill_id, short_description, is_active, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        current_time = datetime.now()
        data = (date_text, code, slug, skill_id, f"Current Affairs for {date_text}", 1, current_time, current_time)
        
        cursor.execute(query, data)
        connection.commit()
        topic_id = cursor.lastrowid
        
        print(f"✅ Created new topic '{date_text}' with ID: {topic_id}")
        cursor.close()
        return topic_id
        
    except mysql.connector.Error as err:
        print(f"❌ Error in get_or_create_topic: {err}")
        # Try to reconnect on connection error
        if "MySQL Connection not available" in str(err) or "Not connected" in str(err):
            global mysql_connection
            mysql_connection = None
        return None

def insert_question(connection, question_data, skill_id, topic_id, translated_question, translated_options, translated_explanation):
    """Insert a question into the questions table"""
    try:
        # Ensure we have a valid connection
        if not connection or not connection.is_connected():
            connection = get_connection()
            if not connection:
                print("❌ Cannot establish MySQL connection for question insertion")
                return None
        
        cursor = connection.cursor()
        
        # Generate a unique question code
        question_code = generate_random_code("que_")
        
        # Prepare the question text with HTML tags
        question_html = f"<p>{translated_question}</p>"
        
        # Prepare options in the required format
        options_data = []
        for idx, option_text in enumerate(translated_options):
            options_data.append({
                "option": option_text,
                "partial_weightage": 0
            })
        options_json = json.dumps(options_data)
        
        # Prepare correct answer in the required format
        correct_answer = f"i:{question_data['correct_answer_index']};"
        
        # Prepare solution/explanation with HTML tags
        solution_html = f"<p>{translated_explanation}</p>"
        
        # Database query
        query = """
        INSERT INTO questions (
            code, question_type_id, question, options, correct_answer, 
            default_marks, default_time, skill_id, topic_id, difficulty_level_id,
            preferences, has_attachment, attachment_type, comprehension_passage_id,
            attachment_options, solution, solution_video, hint,
            avg_time_taken, total_attempts, is_active, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, 
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        """
        
        current_time = datetime.now()
        preferences_json = json.dumps([])
        
        data = (
            question_code, 1, question_html, options_json, correct_answer,
            1, 60, skill_id, topic_id, DIFFICULTY_LEVEL_ID,
            preferences_json, 0, None, None,
            None, solution_html, None, None,
            0, 0, 1, current_time, current_time
        )
        
        cursor.execute(query, data)
        connection.commit()
        question_id = cursor.lastrowid
        
        print(f"✅ Inserted question with ID: {question_id}")
        cursor.close()
        
        # Store mapping in MongoDB for future reference with solution
        questions_collection.insert_one({
            "question_id": question_id,
            "section_id": SECTION_ID,
            "skill_id": skill_id,
            "topic_id": topic_id,
            "created_at": current_time,
            "question": translated_question,
            "correct_answer_index": question_data['correct_answer_index'],
            "options": translated_options,
            "solution": translated_explanation  # Added solution to MongoDB
        })
        
        return question_id
        
    except mysql.connector.Error as err:
        print(f"❌ Error inserting question: {err}")
        # Try to reconnect on connection error
        if "MySQL Connection not available" in str(err) or "Not connected" in str(err):
            global mysql_connection
            mysql_connection = None
        return None

def mark_url_as_processed(url):
    """Mark a URL as processed in MongoDB"""
    # Check if URL already exists in collection
    if scraped_urls_collection.count_documents({"url": url}) == 0:
        scraped_urls_collection.insert_one({
            "url": url, 
            "scraped_at": datetime.now(), 
            "processed": True
        })
        print(f"✅ Marked URL as processed: {url}")
    else:
        print(f"ℹ️ URL already marked as processed: {url}")

def get_processed_urls():
    """Get a list of URLs that have already been processed"""
    processed_urls = scraped_urls_collection.find({"processed": True})
    return [doc["url"] for doc in processed_urls]

def is_url_already_scraped(url):
    """Check if a URL has already been scraped
    
    Args:
        url (str): URL to check
        
    Returns:
        bool: True if URL has already been scraped, False otherwise
    """
    count = scraped_urls_collection.count_documents({"url": url})
    return count > 0

def get_scraping_stats():
    """Get statistics about scraped URLs
    
    Returns:
        dict: Dictionary with statistics
    """
    total_urls = scraped_urls_collection.count_documents({})
    
    # Group by month
    pipeline = [
        {
            "$match": {
                "scraped_at": {"$exists": True, "$ne": None}
            }
        },
        {
            "$group": {
                "_id": {
                    "year": {"$year": "$scraped_at"},
                    "month": {"$month": "$scraped_at"}
                },
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"_id.year": -1, "_id.month": -1}}
    ]
    
    monthly_stats = list(scraped_urls_collection.aggregate(pipeline))
    
    # Format monthly stats
    formatted_stats = []
    for stat in monthly_stats:
        if stat["_id"] and "year" in stat["_id"] and "month" in stat["_id"]:
            year = stat["_id"]["year"]
            month = stat["_id"]["month"]
            if year is not None and month is not None:
                try:
                    month_name = datetime(year, month, 1).strftime('%B')
                    formatted_stats.append({
                        "month": f"{month_name} {year}",
                        "count": stat["count"]
                    })
                except (TypeError, ValueError):
                    # Skip invalid date entries
                    pass
    
    return {
        "total_urls_scraped": total_urls,
        "monthly_breakdown": formatted_stats
    }

def close_connections(connection=None):
    """
    Close MySQL and MongoDB connections
    
    Args:
        connection: Specific MySQL connection to close
    """
    global mysql_connection
    
    try:
        # Close the specific connection if provided
        if connection is not None and hasattr(connection, 'is_connected'):
            try:
                if connection.is_connected():
                    connection.close()
                    print("✅ Specific MySQL connection closed")
            except mysql.connector.Error as err:
                # Handle MySQL specific errors more gracefully
                if err.errno == 2055:  # Lost connection to MySQL server
                    print("ℹ️ Connection was already closed by server (SSL protocol violation)")
                else:
                    print(f"⚠️ Warning when closing specific connection: {err}")
            except Exception as e:
                print(f"⚠️ Warning when closing specific connection: {str(e)}")
        
        # Close the global MySQL connection if exists
        if mysql_connection is not None and hasattr(mysql_connection, 'is_connected'):
            try:
                if mysql_connection.is_connected():
                    mysql_connection.close()
                    mysql_connection = None
                    print("✅ Global MySQL connection closed")
            except mysql.connector.Error as err:
                # Handle MySQL specific errors more gracefully
                if err.errno == 2055:  # Lost connection to MySQL server
                    print("ℹ️ Global connection was already closed by server (SSL protocol violation)")
                else:
                    print(f"⚠️ Warning when closing global connection: {err}")
                # Set to None anyway since we can't use it anymore
                mysql_connection = None
            except Exception as e:
                print(f"⚠️ Warning when closing global connection: {str(e)}")
                mysql_connection = None
    except Exception as e:
        print(f"⚠️ Warning when closing connections: {str(e)}")
        # Continue despite errors to ensure function completes

def test_connection():
    """
    Test database connectivity and report status
    
    This function tries to connect with both SSL enabled and disabled
    to determine which setting works better.
    
    Returns:
        dict: A dictionary with connection test results
    """
    results = {
        "ssl_enabled": {"success": False, "error": None},
        "ssl_disabled": {"success": False, "error": None}
    }
    
    # Test with SSL enabled
    try:
        # Load environment variables
        mysql_host = os.getenv("MYSQL_HOST")
        mysql_user = os.getenv("MYSQL_USER")
        mysql_password = os.getenv("MYSQL_PASSWORD")
        mysql_database = os.getenv("MYSQL_DATABASE")
        
        print("Testing connection with SSL enabled...")
        conn = mysql.connector.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database
        )
        
        print("SSL connection successful!")
        results["ssl_enabled"]["success"] = True
        conn.close()
    except Exception as e:
        print(f"SSL connection failed: {str(e)}")
        results["ssl_enabled"]["error"] = str(e)
    
    # Test with SSL disabled
    try:
        print("\nTesting connection with SSL disabled...")
        conn = mysql.connector.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database,
            ssl_disabled=True
        )
        
        print("Non-SSL connection successful!")
        results["ssl_disabled"]["success"] = True
        conn.close()
    except Exception as e:
        print(f"Non-SSL connection failed: {str(e)}")
        results["ssl_disabled"]["error"] = str(e)
    
    # Print recommendation
    if results["ssl_enabled"]["success"] and results["ssl_disabled"]["success"]:
        print("\nBoth connection methods work. For maximum security, use SSL (MYSQL_VERIFY_SSL=true).")
    elif results["ssl_enabled"]["success"]:
        print("\nOnly SSL connections work. Keep MYSQL_VERIFY_SSL=true.")
    elif results["ssl_disabled"]["success"]:
        print("\nOnly non-SSL connections work. Set MYSQL_VERIFY_SSL=false in your .env file.")
    else:
        print("\nBoth connection methods failed. Check your database credentials and server availability.")
    
    return results 
