import asyncio
import os
import time
from datetime import datetime, timedelta
from twscrape import API, gather  # pyright: ignore[reportMissingImports]
from utils.twitter_util import (
    map_tweet_to_enhanced_tweets,
    insert_enhanced_tweets_to_db,
    map_tweet_user_to_profile,
    insert_twitter_profiles_to_db
)
from utils.scraper_account_util import (
    get_random_twitter_scraper_account,
    mark_account_as_occupied,
    mark_account_as_available,
    mark_account_as_error
)

from dotenv import load_dotenv
load_dotenv()


DEFAULT_USERNAME = os.getenv('DEFAULT_USERNAME')
COOKIE_STRING = os.getenv('COOKIE_STRING')


def get_time_range(start_date, end_date):
    now = datetime.now()
    since = (now - timedelta(days=start_date)).strftime("%Y-%m-%d")
    until = (now - timedelta(days=end_date)).strftime("%Y-%m-%d")
    return since, until


def build_search_query(account_handle, start_date, end_date, min_faves=None, min_replies=None, min_retweets=None):
    since, until = get_time_range(start_date, end_date)
    query = f"{account_handle} since:{since} until:{until}"
    if min_faves:
        query = f"{query} min_faves:{min_faves}"
    if min_replies:
        query = f"{query} min_replies:{min_replies}"
    if min_retweets:
        query = f"{query} min_retweets:{min_retweets}"
    return query


async def scrape_profile(api, account_handle):
    user = await api.user_by_login(account_handle)
    print(f"Found: {user.displayname} (@{user.username})")
    print(f"User profile: {user.dict()}")
    print("=" * 80)
    return user


async def scrape_tweets(api, account_handle):
    query = build_search_query(account_handle=account_handle, start_date=2, end_date=1)
    print(f"Query: {query}")
    tweets = await gather(api.search(query, limit=100))
    print(f"Scraped {len(tweets)} tweets")
    
    # Map tweets to database format
    enhanced_tweets_data = []
    unique_users = {}  # Track unique users to avoid duplicates
    
    for tweet in tweets:
        print(f"Tweet: {tweet.dict()}")
        print("=" * 80)
        
        enhanced_tweet_data = map_tweet_to_enhanced_tweets(tweet)
        enhanced_tweets_data.append(enhanced_tweet_data)
        
        # Collect unique users for profile insertion
        user_id = tweet.user.id_str
        if user_id not in unique_users:
            unique_users[user_id] = tweet.user
    
    # Insert tweets to database
    if enhanced_tweets_data:
        insert_enhanced_tweets_to_db(enhanced_tweets_data)
    
    # Insert/update Twitter profiles after tweets are inserted
    if unique_users:
        profiles_data = []
        for user_data in unique_users.values():
            profile_data = map_tweet_user_to_profile(user_data)
            profiles_data.append(profile_data)
        
        insert_twitter_profiles_to_db(profiles_data)
    
    return tweets

def build_scraper_account(username=None, cookie_string=None, max_retries=5):
    """
    Build scraper account from database or use fallback values
    Retries fetching account from database up to max_retries times
    """
    if username and cookie_string:
        return {
            "username": username,
            "cookie_string": cookie_string
        }
    
    # Try to get account from database with retry logic
    for attempt in range(max_retries):
        try:
            account_data = get_random_twitter_scraper_account()
            if account_data:
                print(f"✅ Successfully fetched account from database (attempt {attempt + 1})")
                return {
                    "username": account_data["username"],
                    "cookie_string": account_data["cookie_string"]
                }
        except Exception as e:
            print(f"❌ Error fetching account from database (attempt {attempt + 1}/{max_retries}): {e}")
            
            # If this is not the last attempt, wait before retrying
            if attempt < max_retries - 1:
                # Exponential backoff: 1s, 2s, 4s, 8s
                delay = 2 ** attempt
                print(f"⏳ Waiting {delay} seconds before retry...")
                time.sleep(delay)
            else:
                print(f"❌ Failed to fetch account after {max_retries} attempts")
    
    # Fallback to default values if database fetch fails after all retries
    print("⚠️ Using fallback account credentials")
    return {
        "username": DEFAULT_USERNAME,
        "cookie_string": COOKIE_STRING
    }

async def main():
    api = API()
    account = build_scraper_account()
    
    # Check if account came from database (not fallback)
    account_from_db = account["username"] != DEFAULT_USERNAME
    error_occurred = False
    
    # Mark account as occupied if it came from database
    if account_from_db:
        mark_account_as_occupied(account["username"])
    
    try:
        await api.pool.add_account(
            username=account["username"],
            password="",  # Required but not used when cookies are provided
            email="",  # Required but not used when cookies are provided
            email_password="",  # Required but not used when cookies are provided
            cookies=account["cookie_string"]
        )
        print(f"Account added successfully with cookies!\n")
    except Exception as e:
        print(f"Error adding account: {e}")
        # Mark account as error if there was an error
        if account_from_db:
            mark_account_as_error(account["username"], str(e))
        error_occurred = True
        return
    
    try:
        # user = await scrape_profile(api, "Decrypting_xyz")
        await scrape_tweets(api, "ostrich_hq")

    except Exception as e:
        print(f"Error scraping tweets: {e}")
        # Mark account as error if there was an error during scraping
        if account_from_db:
            mark_account_as_error(account["username"], str(e))
        error_occurred = True
    finally:
        # Mark account as available after scraping is complete (only if no errors occurred)
        if account_from_db and not error_occurred:
            mark_account_as_available(account["username"])


if __name__ == "__main__":
    asyncio.run(main())