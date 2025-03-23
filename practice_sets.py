import mysql.connector
import pymongo
import os
import random
import string
import json
from datetime import datetime, timedelta
import re
from dotenv import load_dotenv

# Import modules
from db_utils import (
    get_connection,
    close_connections,
    create_slug,
    generate_random_code
)

# Load environment variables
load_dotenv()

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
mongo_client = pymongo.MongoClient(MONGO_URI)
db = mongo_client["CurrentAffairs"]
questions_collection = db["Questions"]

def generate_practice_set_code():
    """Generate a unique practice set code"""
    return generate_random_code("set_")

def get_skill_id_by_name(connection, skill_name):
    """Get skill ID by name
    
    Args:
        connection: MySQL connection
        skill_name (str): Skill name (e.g., "March 2024")
        
    Returns:
        int: Skill ID if found, None otherwise
    """
    try:
        cursor = connection.cursor(dictionary=True)
        query = "SELECT id FROM skills WHERE name = %s AND deleted_at IS NULL"
        cursor.execute(query, (skill_name,))
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            return result['id']
        return None
    except mysql.connector.Error as err:
        print(f"❌ Error in get_skill_id_by_name: {err}")
        return None

def get_topic_id_by_name(connection, topic_name):
    """Get topic ID by name
    
    Args:
        connection: MySQL connection
        topic_name (str): Topic name (e.g., "15 March 2024 Current Affairs")
        
    Returns:
        int: Topic ID if found, None otherwise
    """
    try:
        cursor = connection.cursor(dictionary=True)
        query = "SELECT id FROM topics WHERE name = %s AND deleted_at IS NULL"
        cursor.execute(query, (topic_name,))
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            return result['id']
        return None
    except mysql.connector.Error as err:
        print(f"❌ Error in get_topic_id_by_name: {err}")
        return None

def count_questions_for_topic(topic_id):
    """Count questions for a specific topic in MongoDB
    
    Args:
        topic_id (int): Topic ID
        
    Returns:
        int: Number of questions
    """
    return questions_collection.count_documents({"topic_id": topic_id})

def get_questions_for_topic(topic_id):
    """Get questions for a specific topic from MongoDB
    
    Args:
        topic_id (int): Topic ID
        
    Returns:
        list: List of question IDs
    """
    questions = questions_collection.find({"topic_id": topic_id})
    return [q["question_id"] for q in questions]

def get_questions_for_skill(skill_id):
    """Get questions for a specific skill from MongoDB
    
    Args:
        skill_id (int): Skill ID
        
    Returns:
        list: List of question IDs
    """
    questions = questions_collection.find({"skill_id": skill_id})
    return [q["question_id"] for q in questions]

def get_questions_for_date_range(start_date, end_date):
    """Get questions for a specific date range from MongoDB
    
    Args:
        start_date (datetime): Start date
        end_date (datetime): End date
        
    Returns:
        list: List of question IDs
    """
    questions = questions_collection.find({
        "created_at": {
            "$gte": start_date,
            "$lte": end_date
        }
    })
    return [q["question_id"] for q in questions]

def create_practice_set(
    connection, 
    title, 
    skill_id, 
    total_questions, 
    sub_category_id=2,
    description=None
):
    """Create a practice set
    
    Args:
        connection: MySQL connection
        title (str): Practice set title
        skill_id (int): Skill ID
        total_questions (int): Total number of questions
        sub_category_id (int): Sub-category ID
        description (str): Practice set description
        
    Returns:
        int: Practice set ID if created, None otherwise
    """
    try:
        cursor = connection.cursor()
        
        # Generate code and slug
        code = generate_practice_set_code()
        slug = create_slug(title)
        
        # Create description if not provided
        if not description:
            description = f"Practice set for {title}. This set contains {total_questions} questions based on current affairs."
        
        # Settings
        settings = json.dumps({"show_reward_popup": True})
        
        # Current timestamp
        current_time = datetime.now()
        
        # Insert practice set
        query = """
        INSERT INTO practice_sets (
            title, slug, code, sub_category_id, skill_id, description, 
            total_questions, auto_grading, correct_marks, allow_rewards, 
            settings, is_paid, price, is_active, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s
        )
        """
        
        data = (
            title, slug, code, sub_category_id, skill_id, description,
            total_questions, 1, None, 1,
            settings, 0, None, 1, current_time, current_time
        )
        
        cursor.execute(query, data)
        connection.commit()
        practice_set_id = cursor.lastrowid
        
        print(f"✅ Created practice set '{title}' with ID: {practice_set_id}")
        cursor.close()
        return practice_set_id
        
    except mysql.connector.Error as err:
        print(f"❌ Error creating practice set: {err}")
        return None

def add_questions_to_practice_set(connection, practice_set_id, question_ids):
    """Add questions to a practice set
    
    Args:
        connection: MySQL connection
        practice_set_id (int): Practice set ID
        question_ids (list): List of question IDs
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        cursor = connection.cursor()
        
        # Add each question to the practice set
        success_count = 0
        for question_id in question_ids:
            try:
                # Insert question into practice_set_questions
                query = """
                INSERT INTO practice_set_questions (
                    practice_set_id, question_id
                ) VALUES (%s, %s)
                """
                
                data = (practice_set_id, question_id)
                
                cursor.execute(query, data)
                connection.commit()
                success_count += 1
            except mysql.connector.Error as err:
                print(f"❌ Error adding question {question_id} to practice set: {err}")
        
        print(f"✅ Added {success_count}/{len(question_ids)} questions to practice set {practice_set_id}")
        cursor.close()
        return success_count == len(question_ids)
        
    except mysql.connector.Error as err:
        print(f"❌ Error adding questions to practice set: {err}")
        return False

def create_daily_practice_set(connection, date_text, topic_name, skill_name):
    """Create a practice set for a specific day
    
    Args:
        connection: MySQL connection
        date_text (str): Date text (e.g., "15 March 2024")
        topic_name (str): Topic name (e.g., "15 March 2024 Current Affairs")
        skill_name (str): Skill name (e.g., "March 2024")
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get skill ID
        skill_id = get_skill_id_by_name(connection, skill_name)
        if not skill_id:
            print(f"❌ Skill '{skill_name}' not found")
            return False
        
        # Get topic ID
        topic_id = get_topic_id_by_name(connection, topic_name)
        if not topic_id:
            print(f"❌ Topic '{topic_name}' not found")
            return False
        
        # Get questions for topic
        question_ids = get_questions_for_topic(topic_id)
        total_questions = len(question_ids)
        
        if total_questions == 0:
            print(f"❌ No questions found for topic '{topic_name}'")
            return False
        
        # Create practice set title
        title = f"{date_text} Current Affairs"
        
        # Create description
        description = f"Practice set for {date_text} Current Affairs. This set contains {total_questions} questions to test your knowledge of current events from {date_text}."
        
        # Create practice set
        practice_set_id = create_practice_set(
            connection,
            title,
            skill_id,
            total_questions,
            description=description
        )
        
        if not practice_set_id:
            print(f"❌ Failed to create practice set for '{title}'")
            return False
        
        # Add questions to practice set
        return add_questions_to_practice_set(connection, practice_set_id, question_ids)
        
    except Exception as e:
        print(f"❌ Error creating daily practice set: {e}")
        return False

def create_monthly_practice_set(connection, month_year):
    """Create a practice set for a specific month
    
    Args:
        connection: MySQL connection
        month_year (str): Month and year (e.g., "March 2024")
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get skill ID
        skill_id = get_skill_id_by_name(connection, month_year)
        if not skill_id:
            print(f"❌ Skill '{month_year}' not found")
            return False
        
        # Get questions for skill
        question_ids = get_questions_for_skill(skill_id)
        total_questions = len(question_ids)
        
        if total_questions == 0:
            print(f"❌ No questions found for month '{month_year}'")
            return False
        
        # Create practice set title
        title = f"{month_year} Monthly Current Affairs"
        
        # Create description
        description = f"Monthly practice set for {month_year} Current Affairs. This comprehensive set contains {total_questions} questions covering all important current events from {month_year}."
        
        # Create practice set
        practice_set_id = create_practice_set(
            connection,
            title,
            skill_id,
            total_questions,
            description=description
        )
        
        if not practice_set_id:
            print(f"❌ Failed to create practice set for '{title}'")
            return False
        
        # Add questions to practice set
        return add_questions_to_practice_set(connection, practice_set_id, question_ids)
        
    except Exception as e:
        print(f"❌ Error creating monthly practice set: {e}")
        return False

def create_weekly_practice_set(connection):
    """Create a practice set for the past week
    
    Args:
        connection: MySQL connection
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Calculate date range (past 7 days)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        # Format dates for display
        start_date_text = start_date.strftime("%d %B %Y")
        end_date_text = end_date.strftime("%d %B %Y")
        
        # Get questions for date range
        question_ids = get_questions_for_date_range(start_date, end_date)
        total_questions = len(question_ids)
        
        if total_questions == 0:
            print(f"❌ No questions found for the past week ({start_date_text} to {end_date_text})")
            return False
        
        # Create practice set title
        title = f"Weekly Current Affairs ({start_date_text} to {end_date_text})"
        
        # Get most recent skill ID (approximation)
        cursor = connection.cursor(dictionary=True)
        query = "SELECT id FROM skills WHERE deleted_at IS NULL ORDER BY created_at DESC LIMIT 1"
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        
        if not result:
            print(f"❌ No skills found in the database")
            return False
        
        skill_id = result['id']
        
        # Create description
        description = f"Weekly practice set covering current affairs from {start_date_text} to {end_date_text}. This set contains {total_questions} questions to test your knowledge of recent events."
        
        # Create practice set
        practice_set_id = create_practice_set(
            connection,
            title,
            skill_id,
            total_questions,
            description=description
        )
        
        if not practice_set_id:
            print(f"❌ Failed to create practice set for '{title}'")
            return False
        
        # Add questions to practice set
        return add_questions_to_practice_set(connection, practice_set_id, question_ids)
        
    except Exception as e:
        print(f"❌ Error creating weekly practice set: {e}")
        return False

def create_date_range_practice_set(connection, start_date, end_date):
    """Create a practice set for a specific date range
    
    Args:
        connection: MySQL connection
        start_date (datetime): Start date
        end_date (datetime): End date
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Format dates for display
        start_date_text = start_date.strftime("%d %B %Y")
        end_date_text = end_date.strftime("%d %B %Y")
        
        # Get questions for date range
        question_ids = get_questions_for_date_range(start_date, end_date)
        total_questions = len(question_ids)
        
        if total_questions == 0:
            print(f"❌ No questions found for the date range ({start_date_text} to {end_date_text})")
            return False
        
        # Create practice set title
        title = f"Current Affairs ({start_date_text} to {end_date_text})"
        
        # Get skill from start date month
        start_month_year = start_date.strftime("%B %Y")
        skill_id = get_skill_id_by_name(connection, start_month_year)
        
        if not skill_id:
            # Fallback to most recent skill
            cursor = connection.cursor(dictionary=True)
            query = "SELECT id FROM skills WHERE deleted_at IS NULL ORDER BY created_at DESC LIMIT 1"
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            
            if not result:
                print(f"❌ No skills found in the database")
                return False
            
            skill_id = result['id']
        
        # Create description
        description = f"Practice set covering current affairs from {start_date_text} to {end_date_text}. This set contains {total_questions} questions from this date range."
        
        # Create practice set
        practice_set_id = create_practice_set(
            connection,
            title,
            skill_id,
            total_questions,
            description=description
        )
        
        if not practice_set_id:
            print(f"❌ Failed to create practice set for '{title}'")
            return False
        
        # Add questions to practice set
        return add_questions_to_practice_set(connection, practice_set_id, question_ids)
        
    except Exception as e:
        print(f"❌ Error creating date range practice set: {e}")
        return False 