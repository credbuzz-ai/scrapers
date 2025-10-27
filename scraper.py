import asyncio
from datetime import datetime, timedelta
from twscrape import API, gather
from utils.twitter_utils import (
    map_tweet_to_enhanced_tweets,
    insert_enhanced_tweets_to_db,
    map_tweet_user_to_profile,
    insert_twitter_profiles_to_db
)
from utils.scraper_account_utils import (
    get_random_twitter_scraper_account,
    mark_account_as_occupied,
    mark_account_as_available,
    mark_account_as_error
)


DEFAULT_USERNAME = "JoyceDavis2750"
COOKIE_STRING = "guest_id_marketing=v1%3A174825289399544801; guest_id_ads=v1%3A174825289399544801; personalization_id='v1_uIVBMfYG6jt+ncdiynAIzA=='; guest_id=v1%3A174825289399544801; __cf_bm=W4soYfkGxLk3f2giKafgSnatDMhSURGylBqzf2OUqAo-1748252894-1.0.1.1-O6kyM7sD0zZtg6p.3oUxxxm_8t9GMFcKo.y5UMTjL.wyo0WUEjH5JMmivlb7gOtSN_Y2Ep1SloOpfdAS7RhrPgoq8Yo.pqIEyE6X1hc0uWg; gt=1926938416861606278; kdt=ea83TP1KRXM2PpkO84fddbdkbaq10f5q2TeHC6mx; twid=u%3D1652494882877976577; ct0=395642ee25e379c514a8e396086ec489988bb5959c7cf34c388afe27e616ce1237d247a464f5721bc66520c8cdca4c7ba7017689af27cdc33e9efaaa74b28b2181a0e45cd273c2f01c94876d788410ac; auth_token=ce5cd10d865c0bc20ae0e58a4d829d7c32d99686"


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

def build_scraper_account(username=None, cookie_string=None):
    """
    Build scraper account from database or use fallback values
    """
    if username and cookie_string:
        return {
            "username": username,
            "cookie_string": cookie_string
        }
    
    # Try to get account from database
    account_data = get_random_twitter_scraper_account()
    if account_data:
        return {
            "username": account_data["username"],
            "cookie_string": account_data["cookie_string"]
        }
    
    # Fallback to default values if database fetch fails
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