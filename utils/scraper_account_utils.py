"""
Scraper account management utilities
Handles fetching and managing Twitter scraper accounts from database
"""

from db_configs import db
from constants import (
    GET_RANDOM_TWITTER_SCRAPER_ACCOUNT_QUERY,
    MARK_ACCOUNT_AS_OCCUPIED_QUERY,
    MARK_ACCOUNT_AS_AVAILABLE_QUERY,
    MARK_ACCOUNT_AS_ERROR_QUERY
)


def get_random_twitter_scraper_account():
    """
    Fetch a random Twitter scraper account from configs.twitter_scrapers table
    Returns a dictionary with username and cookie_string
    """
    try:
        results = db.fetch_query(GET_RANDOM_TWITTER_SCRAPER_ACCOUNT_QUERY)
        
        if not results:
            print("❌ No available Twitter scraper accounts found")
            return None
            
        username, cookie, status, is_occupied, lock_time = results[0]
        
        if not cookie:
            print(f"❌ No cookie found for username: {username}")
            return None
            
        print(f"✅ Selected Twitter scraper account: {username}")
        
        return {
            "username": username,
            "cookie_string": cookie,
            "status": status,
            "is_occupied": is_occupied,
            "lock_time": lock_time
        }
        
    except Exception as e:
        print(f"❌ Error fetching Twitter scraper account: {e}")
        return None


def mark_account_as_occupied(username):
    """
    Mark a Twitter scraper account as occupied and update lock_time
    """
    try:
        db.execute_query(MARK_ACCOUNT_AS_OCCUPIED_QUERY, (username,))
        print(f"✅ Marked account {username} as occupied")
        
    except Exception as e:
        print(f"❌ Error marking account as occupied: {e}")


def mark_account_as_available(username):
    """
    Mark a Twitter scraper account as available (not occupied)
    """
    try:
        db.execute_query(MARK_ACCOUNT_AS_AVAILABLE_QUERY, (username,))
        print(f"✅ Marked account {username} as available")
        
    except Exception as e:
        print(f"❌ Error marking account as available: {e}")


def mark_account_as_error(username, error_message):
    """
    Mark a Twitter scraper account as error and log the error message
    """
    try:
        # Truncate error message to fit in database field (assuming error_message is varchar(256))
        truncated_error = str(error_message)[:250] if error_message else "Unknown error"
        
        db.execute_query(MARK_ACCOUNT_AS_ERROR_QUERY, (truncated_error, username))
        print(f"❌ Marked account {username} as error: {truncated_error}")
        
    except Exception as e:
        print(f"❌ Error marking account as error: {e}")


