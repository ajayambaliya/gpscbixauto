import sys
import time
import json
import random
import os
import mysql.connector
import pymongo
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI")
mongo_client = pymongo.MongoClient(MONGO_URI)
db = mongo_client["CurrentAffairss"]
questions_collection = db["Questionss"]

# MySQL Configuration
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

def create_mysql_connection():
    """Create and return a MySQL connection"""
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        print("✅ MySQL connection established successfully")
        return conn
    except mysql.connector.Error as err:
        print(f"❌ MySQL Connection Error: {err}")
        return None

def close_connections(connection):
    """Close MySQL connection"""
    if connection and connection.is_connected():
        connection.close()
        print("✅ MySQL connection closed")

def get_questions_by_month_year(month_year):
    """
    Get questions from MongoDB that match the given month and year
    
    Args:
        month_year (str): Month and year in format "Month Year" (e.g., "March 2023")
        
    Returns:
        list: List of question IDs
    """
    try:
        # First, find the skill_id for the month-year
        connection = create_mysql_connection()
        if not connection:
            return []
            
        cursor = connection.cursor(dictionary=True)
        query = "SELECT id FROM skills WHERE name = %s AND deleted_at IS NULL"
        cursor.execute(query, (month_year,))
        result = cursor.fetchone()
        
        if not result:
            print(f"❌ No skill found for {month_year}")
            cursor.close()
            close_connections(connection)
            return []
            
        skill_id = result['id']
        cursor.close()
        close_connections(connection)
        
        # Now, find questions with this skill_id in MongoDB
        questions = questions_collection.find({"skill_id": skill_id})
        question_ids = [q["question_id"] for q in questions]
        
        return question_ids
        
    except Exception as e:
        print(f"❌ Error getting questions by month-year: {e}")
        return []

def get_questions_by_date(date_str):
    """
    Get questions from MongoDB that match the given date
    
    Args:
        date_str (str): Date in format "DD Month YYYY" (e.g., "15 March 2023")
        
    Returns:
        list: List of question IDs
    """
    try:
        # Append "Current Affairs" to the date string to match topic name
        topic_name = f"{date_str} Current Affairs"
        
        # Find the topic_id for the date
        connection = create_mysql_connection()
        if not connection:
            return []
            
        cursor = connection.cursor(dictionary=True)
        query = "SELECT id FROM topics WHERE name = %s AND deleted_at IS NULL"
        cursor.execute(query, (topic_name,))
        result = cursor.fetchone()
        
        if not result:
            print(f"❌ No topic found for {date_str}")
            cursor.close()
            close_connections(connection)
            return []
            
        topic_id = result['id']
        cursor.close()
        close_connections(connection)
        
        # Now, find questions with this topic_id in MongoDB
        questions = questions_collection.find({"topic_id": topic_id})
        question_ids = [q["question_id"] for q in questions]
        
        return question_ids
        
    except Exception as e:
        print(f"❌ Error getting questions by date: {e}")
        return []

def get_question_details(question_ids):
    """
    Get details of questions from MySQL database
    
    Args:
        question_ids (list): List of question IDs
        
    Returns:
        list: List of question details
    """
    try:
        if not question_ids:
            return []
            
        connection = create_mysql_connection()
        if not connection:
            return []
            
        cursor = connection.cursor(dictionary=True)
        
        # Prepare the placeholders for SQL IN operator
        placeholders = ', '.join(['%s'] * len(question_ids))
        query = f"""
        SELECT id, code, question, options, correct_answer, solution, skill_id, topic_id 
        FROM questions 
        WHERE id IN ({placeholders}) AND deleted_at IS NULL
        """
        
        cursor.execute(query, question_ids)
        questions = cursor.fetchall()
        cursor.close()
        close_connections(connection)
        
        return questions
        
    except Exception as e:
        print(f"❌ Error getting question details: {e}")
        return []

def generate_random_quiz(question_ids, num_questions=10):
    """
    Generate a random quiz with the specified number of questions
    
    Args:
        question_ids (list): List of question IDs to choose from
        num_questions (int): Number of questions in the quiz
        
    Returns:
        list: List of selected question IDs
    """
    if not question_ids:
        return []
        
    # Get a random sample of question IDs
    if len(question_ids) <= num_questions:
        return question_ids
    else:
        return random.sample(question_ids, num_questions)

def save_quiz_to_file(questions, filename):
    """
    Save a quiz to a JSON file
    
    Args:
        questions (list): List of question details
        filename (str): Output filename
    """
    try:
        # Prepare questions for JSON export
        quiz_questions = []
        
        for question in questions:
            # Parse options from JSON string
            options_data = json.loads(question['options'])
            options = [option['option'] for option in options_data]
            
            # Extract correct answer index
            # Format is typically "i:X;" where X is the 1-based index
            correct_answer_str = question['correct_answer']
            correct_index = 0
            if correct_answer_str.startswith('i:') and correct_answer_str.endswith(';'):
                try:
                    correct_index = int(correct_answer_str[2:-1]) - 1  # Convert to 0-based index
                except ValueError:
                    correct_index = 0
            
            quiz_questions.append({
                'id': question['id'],
                'code': question['code'],
                'question': question['question'],
                'options': options,
                'correct_index': correct_index,
                'solution': question['solution'],
                'skill_id': question['skill_id'],
                'topic_id': question['topic_id']
            })
        
        # Create quiz data structure
        quiz_data = {
            'title': f"Current Affairs Quiz - {datetime.now().strftime('%d %B %Y')}",
            'created_at': datetime.now().isoformat(),
            'questions': quiz_questions
        }
        
        # Save to file
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(quiz_data, f, ensure_ascii=False, indent=2)
            
        print(f"✅ Quiz saved to {filename}")
        
    except Exception as e:
        print(f"❌ Error saving quiz to file: {e}")

def display_quiz_stats(questions):
    """Display quiz statistics"""
    if not questions:
        print("❌ No questions available for statistics")
        return
        
    print("\n📊 Quiz Statistics:")
    print(f"Total questions: {len(questions)}")
    
    # Count questions by skill
    skills = {}
    for question in questions:
        skill_id = question['skill_id']
        if skill_id in skills:
            skills[skill_id] += 1
        else:
            skills[skill_id] = 1
    
    if skills:
        print("\nQuestions by Skill:")
        for skill_id, count in skills.items():
            print(f"Skill ID {skill_id}: {count} questions")
    
    # Count questions by topic
    topics = {}
    for question in questions:
        topic_id = question['topic_id']
        if topic_id in topics:
            topics[topic_id] += 1
        else:
            topics[topic_id] = 1
    
    if topics:
        print("\nQuestions by Topic:")
        for topic_id, count in topics.items():
            print(f"Topic ID {topic_id}: {count} questions")
    
def main():
    """Main function to coordinate the quiz generation process"""
    try:
        print("🚀 Starting Quiz Generator")
        
        # Check if parameters are provided
        if len(sys.argv) < 2:
            print("❌ Insufficient parameters. Please provide a quiz type.")
            print("Usage: python quiz_generator.py <type> [additional_params]")
            print("Types:")
            print("  - month <month_year>: Generate a quiz for a specific month and year")
            print("      Example: python quiz_generator.py month \"March 2023\"")
            print("  - date <date>: Generate a quiz for a specific date")
            print("      Example: python quiz_generator.py date \"15 March 2023\"")
            print("  - week: Generate a quiz for the past week")
            print("  - month_auto: Generate a quiz for the current month")
            return
        
        quiz_type = sys.argv[1].lower()
        
        if quiz_type == "month" and len(sys.argv) >= 3:
            month_year = sys.argv[2]
            print(f"📅 Generating quiz for month: {month_year}")
            
            question_ids = get_questions_by_month_year(month_year)
            if not question_ids:
                print(f"❌ No questions found for {month_year}")
                return
                
            print(f"✅ Found {len(question_ids)} questions for {month_year}")
            
            # Generate a random quiz
            selected_ids = generate_random_quiz(question_ids, 10)
            questions = get_question_details(selected_ids)
            
            # Display quiz statistics
            display_quiz_stats(questions)
            
            # Save quiz to file
            filename = f"quiz_{month_year.replace(' ', '_').lower()}.json"
            save_quiz_to_file(questions, filename)
            
        elif quiz_type == "date" and len(sys.argv) >= 3:
            date_str = sys.argv[2]
            print(f"📅 Generating quiz for date: {date_str}")
            
            question_ids = get_questions_by_date(date_str)
            if not question_ids:
                print(f"❌ No questions found for {date_str}")
                return
                
            print(f"✅ Found {len(question_ids)} questions for {date_str}")
            
            # Get all questions for this date
            questions = get_question_details(question_ids)
            
            # Display quiz statistics
            display_quiz_stats(questions)
            
            # Save quiz to file
            filename = f"quiz_{date_str.replace(' ', '_').lower()}.json"
            save_quiz_to_file(questions, filename)
            
        elif quiz_type == "week":
            print("📅 Generating quiz for the past week")
            
            # Calculate dates for the past week
            today = datetime.now()
            one_week_ago = today - timedelta(days=7)
            
            # Get questions for each day of the past week
            all_question_ids = []
            
            for i in range(7):
                current_date = one_week_ago + timedelta(days=i)
                date_str = current_date.strftime("%d %B %Y")
                
                print(f"🔍 Checking questions for {date_str}")
                day_question_ids = get_questions_by_date(date_str)
                
                if day_question_ids:
                    print(f"✅ Found {len(day_question_ids)} questions for {date_str}")
                    all_question_ids.extend(day_question_ids)
            
            if not all_question_ids:
                print("❌ No questions found for the past week")
                return
                
            print(f"✅ Found a total of {len(all_question_ids)} questions for the past week")
            
            # Generate a random quiz
            selected_ids = generate_random_quiz(all_question_ids, 15)
            questions = get_question_details(selected_ids)
            
            # Display quiz statistics
            display_quiz_stats(questions)
            
            # Save quiz to file
            week_end = today.strftime("%d_%b")
            week_start = one_week_ago.strftime("%d_%b")
            filename = f"weekly_quiz_{week_start}_to_{week_end}.json"
            save_quiz_to_file(questions, filename)
            
        elif quiz_type == "month_auto":
            print("📅 Generating quiz for the current month")
            
            # Get current month and year
            current_month_year = datetime.now().strftime("%B %Y")
            
            print(f"🔍 Checking questions for {current_month_year}")
            question_ids = get_questions_by_month_year(current_month_year)
            
            if not question_ids:
                print(f"❌ No questions found for {current_month_year}")
                return
                
            print(f"✅ Found {len(question_ids)} questions for {current_month_year}")
            
            # Generate a random quiz
            selected_ids = generate_random_quiz(question_ids, 10)
            questions = get_question_details(selected_ids)
            
            # Display quiz statistics
            display_quiz_stats(questions)
            
            # Save quiz to file
            filename = f"monthly_quiz_{current_month_year.replace(' ', '_').lower()}.json"
            save_quiz_to_file(questions, filename)
            
        else:
            print("❌ Invalid quiz type or missing parameters")
            print("Usage: python quiz_generator.py <type> [additional_params]")
            print("Types: month, date, week, month_auto")
            
    except Exception as e:
        print(f"❌ Error in main process: {e}")
    finally:
        print("\n✅ Quiz generation process completed")

if __name__ == "__main__":
    main() 
