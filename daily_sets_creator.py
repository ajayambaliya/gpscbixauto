#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script to create individual daily practice sets for each day in a date range.
This is used by the scrape_and_practice.bat file when the user chooses to create
individual practice sets after scraping a date range.
"""

import sys
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time
from practice_sets import create_practice_set_for_date, get_questions_for_date

def create_daily_sets(start_year, start_month, start_day, end_year, end_month, end_day):
    """Create individual daily practice sets for each day in a date range."""
    try:
        # Convert input parameters to integers
        start_year = int(start_year)
        start_month = int(start_month)
        start_day = int(start_day)
        end_year = int(end_year)
        end_month = int(end_month)
        end_day = int(end_day)
        
        # Create datetime objects for start and end dates
        start_date = datetime(start_year, start_month, start_day)
        end_date = datetime(end_year, end_month, end_day)
        
        # Validate date range
        if end_date < start_date:
            print("Error: End date must be after start date.")
            return False
        
        # Count number of days in the range
        delta = end_date - start_date
        total_days = delta.days + 1
        
        print(f"Processing {total_days} days from {start_date.strftime('%d %B %Y')} to {end_date.strftime('%d %B %Y')}")
        
        # Initialize counters
        created_count = 0
        skipped_count = 0
        
        # Iterate through each day in the range
        current_date = start_date
        while current_date <= end_date:
            # Extract day, month, year
            current_year = current_date.year
            current_month = current_date.month
            current_day = current_date.day
            
            # Check if there are questions for this date
            questions = get_questions_for_date(current_year, current_month, current_day)
            
            if questions and len(questions) > 0:
                # Try to create a practice set for this date
                date_str = current_date.strftime("%d %B %Y")
                print(f"\nProcessing date: {date_str} - Found {len(questions)} questions")
                
                result = create_practice_set_for_date(current_year, current_month, current_day)
                if result:
                    print(f"✓ Created practice set for {date_str}")
                    created_count += 1
                else:
                    print(f"✗ Failed to create practice set for {date_str}")
                    skipped_count += 1
            else:
                print(f"\nSkipping {current_date.strftime('%d %B %Y')} - No questions found")
                skipped_count += 1
            
            # Move to the next day
            current_date += timedelta(days=1)
            
            # Add a small delay to prevent overloading the database
            time.sleep(0.5)
        
        # Print summary
        print("\n" + "="*50)
        print(f"Summary: Processed {total_days} days")
        print(f"Created: {created_count} practice sets")
        print(f"Skipped: {skipped_count} days (no questions or error)")
        print("="*50)
        
        return True
        
    except ValueError as e:
        print(f"Error parsing date parameters: {str(e)}")
        return False
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return False

def print_usage():
    """Print usage information for the script."""
    print("Usage: python daily_sets_creator.py <start_year> <start_month> <start_day> <end_year> <end_month> <end_day>")
    print("Example: python daily_sets_creator.py 2024 5 1 2024 5 15")

def main():
    """Main function to handle command-line arguments and create daily practice sets."""
    # Load environment variables
    load_dotenv()
    
    # Check if correct number of arguments is provided
    if len(sys.argv) != 7:
        print("Error: Incorrect number of arguments.")
        print_usage()
        return 1
    
    try:
        # Extract parameters
        start_year = sys.argv[1]
        start_month = sys.argv[2]
        start_day = sys.argv[3]
        end_year = sys.argv[4]
        end_month = sys.argv[5]
        end_day = sys.argv[6]
        
        # Create daily practice sets
        result = create_daily_sets(start_year, start_month, start_day, end_year, end_month, end_day)
        
        # Return appropriate exit code
        return 0 if result else 1
        
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 