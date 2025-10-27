"""
Scrape Twitter using cookies from browser console
Method 1: Copy cookies from browser console (document.cookie)
"""

import asyncio
from datetime import datetime, timedelta
from twscrape import API, gather
from utils import (
    map_tweet_to_enhanced_tweets,
    insert_enhanced_tweets_to_db,
    map_tweet_user_to_profile,
    insert_twitter_profiles_to_db
)


DEFAULT_USERNAME = "basetillmoon"
COOKIE_STRING = "ct0=6cda6d5edf6568366441274d918b3de1542c7d5c880c900c0a068bdd6d2ba54a0f0e883c98f1ba952b55c575950261c7bd0f622ccb5848923aba844cb00bb5e00a36304473c66c3657ecd4747c85d1db; auth_token=f8e615c2c0882493d813424f87342794c647f0a8; gt=1982245615540244612; personalization_id=v1_gUH1DI7CnEj91bOpXQQNQQ==; twid=u%3D1520825759916560385; guest_id=v1%3A176144024780219953"

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

def build_scraper_account(username=DEFAULT_USERNAME, cookie_string=COOKIE_STRING):
    return {
        "username": username,
        "cookie_string": cookie_string
    }

async def main():
    api = API()
    account = build_scraper_account()
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
        return
    
    try:
        # user = await scrape_profile(api, "Decrypting_xyz")
        await scrape_tweets(api, "ostrich_hq")

    except Exception as e:
        print(f"Error scraping tweets: {e}")


if __name__ == "__main__":
    asyncio.run(main())