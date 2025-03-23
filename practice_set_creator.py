import sys
import os
from datetime import datetime, date, timedelta
from dotenv import load_dotenv

# Import modules
from db_utils import get_connection, close_connections
from practice_sets import (
    create_daily_practice_set,
    create_monthly_practice_set,
    create_weekly_practice_set,
    create_date_range_practice_set
)

# Load environment variables
load_dotenv()

def print_header(title):
    """Print a formatted header"""
    print("\n" + "=" * 50)
    print(f"{title.center(50)}")
    print("=" * 50 + "\n")

def create_practice_set_for_date(year, month, day):
    """Create a practice set for a specific date"""
    try:
        # Create a date object
        target_date = date(int(year), int(month), int(day))
        
        # Format the date
        date_text = target_date.strftime("%d %B %Y")
        month_year = target_date.strftime("%B %Y")
        topic_name = f"{date_text} Current Affairs"
        
        print_header(f"CREATING PRACTICE SET FOR {date_text}")
        
        # Establish database connection
        connection = get_connection()
        if not connection:
            print("❌ Failed to establish database connection")
            return False
        
        # Create the practice set
        success = create_daily_practice_set(connection, date_text, topic_name, month_year)
        
        # Close connection
        close_connections(connection)
        
        if success:
            print(f"✅ Successfully created practice set for {date_text}")
        else:
            print(f"❌ Failed to create practice set for {date_text}")
            
        return success
    
    except ValueError:
        print("❌ Invalid date format. Please check your input.")
        return False
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

def create_practice_set_for_month(year, month):
    """Create a practice set for a specific month"""
    try:
        # Create a date object for the first day of the month
        first_day = date(int(year), int(month), 1)
        
        # Format the month and year
        month_year = first_day.strftime("%B %Y")
        
        print_header(f"CREATING PRACTICE SET FOR {month_year}")
        
        # Establish database connection
        connection = get_connection()
        if not connection:
            print("❌ Failed to establish database connection")
            return False
        
        # Create the practice set
        success = create_monthly_practice_set(connection, month_year)
        
        # Close connection
        close_connections(connection)
        
        if success:
            print(f"✅ Successfully created practice set for {month_year}")
        else:
            print(f"❌ Failed to create practice set for {month_year}")
            
        return success
    
    except ValueError:
        print("❌ Invalid date format. Please check your input.")
        return False
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

def create_practice_set_for_week():
    """Create a practice set for the past week"""
    try:
        print_header("CREATING WEEKLY PRACTICE SET")
        
        # Establish database connection
        connection = get_connection()
        if not connection:
            print("❌ Failed to establish database connection")
            return False
        
        # Create the practice set
        success = create_weekly_practice_set(connection)
        
        # Close connection
        close_connections(connection)
        
        if success:
            print("✅ Successfully created weekly practice set")
        else:
            print("❌ Failed to create weekly practice set")
            
        return success
    
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

def create_practice_set_for_range(start_year, start_month, start_day, end_year, end_month, end_day):
    """Create a practice set for a date range"""
    try:
        # Create date objects
        start_date = date(int(start_year), int(start_month), int(start_day))
        end_date = date(int(end_year), int(end_month), int(end_day))
        
        # Validate date range
        if start_date > end_date:
            print("❌ Start date cannot be later than end date.")
            return False
        
        # Format the dates
        start_date_text = start_date.strftime("%d %B %Y")
        end_date_text = end_date.strftime("%d %B %Y")
        
        print_header(f"CREATING PRACTICE SET FOR {start_date_text} TO {end_date_text}")
        
        # Establish database connection
        connection = get_connection()
        if not connection:
            print("❌ Failed to establish database connection")
            return False
        
        # Create the practice set
        success = create_date_range_practice_set(connection, start_date, end_date)
        
        # Close connection
        close_connections(connection)
        
        if success:
            print(f"✅ Successfully created practice set for {start_date_text} to {end_date_text}")
        else:
            print(f"❌ Failed to create practice set for {start_date_text} to {end_date_text}")
            
        return success
    
    except ValueError:
        print("❌ Invalid date format. Please check your input.")
        return False
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

def print_usage():
    """Print usage information"""
    print("Usage:")
    print("  python practice_set_creator.py date <year> <month> <day>")
    print("  python practice_set_creator.py month <year> <month>")
    print("  python practice_set_creator.py week")
    print("  python practice_set_creator.py range <start_year> <start_month> <start_day> <end_year> <end_month> <end_day>")

def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("❌ Insufficient arguments.")
        print_usage()
        return 1
    
    command = sys.argv[1].lower()
    
    if command == "date":
        if len(sys.argv) < 5:
            print("❌ Insufficient arguments for date command.")
            print_usage()
            return 1
        
        year = sys.argv[2]
        month = sys.argv[3]
        day = sys.argv[4]
        
        if create_practice_set_for_date(year, month, day):
            return 0
        return 1
    
    elif command == "month":
        if len(sys.argv) < 4:
            print("❌ Insufficient arguments for month command.")
            print_usage()
            return 1
        
        year = sys.argv[2]
        month = sys.argv[3]
        
        if create_practice_set_for_month(year, month):
            return 0
        return 1
    
    elif command == "week":
        if create_practice_set_for_week():
            return 0
        return 1
    
    elif command == "range":
        if len(sys.argv) < 8:
            print("❌ Insufficient arguments for range command.")
            print_usage()
            return 1
        
        start_year = sys.argv[2]
        start_month = sys.argv[3]
        start_day = sys.argv[4]
        end_year = sys.argv[5]
        end_month = sys.argv[6]
        end_day = sys.argv[7]
        
        if create_practice_set_for_range(start_year, start_month, start_day, end_year, end_month, end_day):
            return 0
        return 1
    
    else:
        print(f"❌ Unknown command: {command}")
        print_usage()
        return 1

if __name__ == "__main__":
    sys.exit(main()) 