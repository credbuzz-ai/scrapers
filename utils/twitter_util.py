"""
Utility functions for Twitter scraper
"""

from datetime import datetime
from db_configs import db
from constants import INSERT_INTO_ENHANCED_TWEETS_QUERY, INSERT_INTO_TWITTER_PROFILES_QUERY


def extract_photo_urls(media):
    """Extract photo URLs from tweet media"""
    if not media:
        return None
    
    photo_urls = []
    
    # Handle different media object structures
    try:
        # Check if media has photos attribute (like in your sample data)
        if hasattr(media, 'photos') and media.photos:
            for photo in media.photos:
                if hasattr(photo, 'url'):
                    photo_urls.append(photo.url)
        
        # Check if media is a list/iterable
        elif hasattr(media, '__iter__') and not isinstance(media, str):
            for item in media:
                if hasattr(item, 'type') and item.type == 'photo':
                    if hasattr(item, 'url'):
                        photo_urls.append(item.url)
        
        # Check if media has url attribute directly
        elif hasattr(media, 'url'):
            photo_urls.append(media.url)
            
    except Exception as e:
        print(f"Warning: Error extracting photo URLs: {e}")
        return None
    
    return ','.join(photo_urls) if photo_urls else None


def map_tweet_to_enhanced_tweets(tweet, script_type="test_scraper"):
    """Map tweet object to twitter.enhanced_tweets table format"""
    # Handle retweeted content
    body = tweet.rawContent
    author_handle = tweet.user.username
    
    if hasattr(tweet, 'retweetedTweet') and tweet.retweetedTweet:
        body = tweet.retweetedTweet.rawContent
        author_handle = tweet.retweetedTweet.user.username
    
    # Extract images
    images = extract_photo_urls(getattr(tweet, "media", None))
    
    # Extract cashtags, hashtags, mentions, links
    cashtags = getattr(tweet, 'cashtags', [])
    hashtags = getattr(tweet, 'hashtags', [])
    mentioned_users = getattr(tweet, 'mentionedUsers', [])
    links = getattr(tweet, 'links', [])
    
    # Count various elements
    number_of_cashtags = len(cashtags) if cashtags else 0
    number_of_hashtags = len(hashtags) if hashtags else 0
    number_of_mentions = len(mentioned_users) if mentioned_users else 0
    number_of_links = len(links) if links else 0
    
    # Get first and main cashtag
    first_cashtag = cashtags[0] if cashtags else None
    main_cashtag = cashtags[0] if cashtags else None  # Assuming first is main
    
    # Determine tweet type
    tweet_type = "original"
    if getattr(tweet, 'retweetedTweet', None):
        tweet_type = "retweet"
    elif getattr(tweet, 'quotedTweet', None):
        tweet_type = "quote"
    elif getattr(tweet, 'inReplyToTweetId', None):
        tweet_type = "reply"
    
    # Check if body is full (not truncated)
    is_full_body = len(body) < 280  # Twitter character limit
    
    # Count contracts (assuming this refers to smart contract addresses)
    # This would need custom logic based on your requirements
    number_of_contracts = 0
    
    return {
        "tweet_id": str(tweet.id_str),
        "author_id": tweet.user.id_str,
        "body": body,
        "author_handle": author_handle,
        "tweet_create_time": tweet.date,
        "create_time": datetime.now(),
        "retweet_count": getattr(tweet, 'retweetCount', 0),
        "like_count": getattr(tweet, 'likeCount', 0),
        "reply_count": getattr(tweet, 'replyCount', 0),
        "quote_count": getattr(tweet, 'quoteCount', 0),
        "view_count": getattr(tweet, 'viewCount', 0),
        "update_time": datetime.now(),
        "profile_image_url": tweet.user.profileImageUrl,
        "is_hidden": 0,  # Default to not hidden
        "impressions": getattr(tweet, 'viewCount', 0),  # Using view count as impressions
        "matching_values": None,  # JSON field, can be populated later
        "is_mapped": 0,  # Default to not mapped
        "sentiment": None,  # Can be populated by sentiment analysis
        "source": getattr(tweet, 'source', None),
        "scraped_by": script_type,
        "number_of_cashtags": number_of_cashtags,
        "tweet_category": tweet_type,
        "first_cashtag": first_cashtag,
        "main_cashtag": main_cashtag,
        "number_of_hashtags": number_of_hashtags,
        "number_of_mentions": number_of_mentions,
        "number_of_contracts": number_of_contracts,
        "number_of_links": number_of_links,
        "is_full_body": 1 if is_full_body else 0,
        "sentiment_new": None,  # Float field for sentiment score
        "is_reply": 1 if getattr(tweet, 'inReplyToTweetId', None) else 0,
        "reply_to": str(tweet.inReplyToTweetId) if getattr(tweet, 'inReplyToTweetId', None) else None,
        "is_quote": 1 if getattr(tweet, 'quotedTweet', None) else 0,
        "quoted_to": tweet.quotedTweet.id_str if getattr(tweet, 'quotedTweet', None) else None,
        "images": images,
    }


def insert_enhanced_tweets_to_db(tweets_data):
    """Insert enhanced tweets data into twitter.enhanced_tweets table"""
    if not tweets_data:
        return
    
    try:
        # Prepare data for batch insert
        values = []
        for tweet_data in tweets_data:
            values.append((
                tweet_data["tweet_id"],
                tweet_data["author_id"],
                tweet_data["body"],
                tweet_data["author_handle"],
                tweet_data["tweet_create_time"],
                tweet_data["create_time"],
                tweet_data["retweet_count"],
                tweet_data["like_count"],
                tweet_data["reply_count"],
                tweet_data["quote_count"],
                tweet_data["view_count"],
                tweet_data["update_time"],
                tweet_data["profile_image_url"],
                tweet_data["is_hidden"],
                tweet_data["impressions"],
                tweet_data["matching_values"],
                tweet_data["is_mapped"],
                tweet_data["sentiment"],
                tweet_data["source"],
                tweet_data["scraped_by"],
                tweet_data["number_of_cashtags"],
                tweet_data["tweet_category"],
                tweet_data["first_cashtag"],
                tweet_data["main_cashtag"],
                tweet_data["number_of_hashtags"],
                tweet_data["number_of_mentions"],
                tweet_data["number_of_contracts"],
                tweet_data["number_of_links"],
                tweet_data["is_full_body"],
                tweet_data["sentiment_new"],
                tweet_data["is_reply"],
                tweet_data["reply_to"],
                tweet_data["is_quote"],
                tweet_data["quoted_to"],
                tweet_data["images"]
            ))
        
        # Execute batch insert
        db.executemany_query(INSERT_INTO_ENHANCED_TWEETS_QUERY, values)
        print(f"✅ Successfully inserted {len(tweets_data)} enhanced tweets into database")
        
    except Exception as e:
        print(f"❌ Error inserting enhanced tweets to database: {e}")


def map_tweet_user_to_profile(user_data, script_type="test_scraper"):
    """Map tweet user data to twitter.twitter_profiles table format"""
    # Extract URL from description links if available
    url_in_bio = None
    if hasattr(user_data, 'descriptionLinks') and user_data.descriptionLinks:
        for link in user_data.descriptionLinks:
            if hasattr(link, 'url'):
                url_in_bio = link.url
                break
    
    # Determine if user is crypto-related based on bio
    is_crypto_user = 0
    bio_text = getattr(user_data, 'rawDescription', '') or ''
    crypto_keywords = ['crypto', 'bitcoin', 'btc', 'ethereum', 'eth', 'blockchain', 'defi', 'nft', 'web3', 'trading', 'token']
    if any(keyword.lower() in bio_text.lower() for keyword in crypto_keywords):
        is_crypto_user = 1
    
    return {
        "author_id": user_data.id_str,
        "name": user_data.displayname,
        "handle": user_data.username,
        "bio": getattr(user_data, 'rawDescription', None),
        "url_in_bio": url_in_bio,
        "profile_image_url": user_data.profileImageUrl,
        "profile_banner_url": getattr(user_data, 'profileBannerUrl', None),
        "followers_count": getattr(user_data, 'followersCount', 0),
        "followings_count": getattr(user_data, 'friendsCount', 0),
        "is_verified": 1 if getattr(user_data, 'verified', False) else 0,
        "account_created_at": getattr(user_data, 'created', None),
        "tag": None,  # Can be populated later
        "is_active": 1,  # Default to active
        "inserted_at": datetime.now(),
        "updated_at": datetime.now(),
        "ai_tag": 0,  # Default to not AI tagged
        "is_processed_by_ai": 0,  # Default to not processed
        "scraped_by": script_type,
        "professional_category": None,  # Can be populated later
        "lifetime_tweets": getattr(user_data, 'statusesCount', 0),
        "lifetime_views": None,  # Not available in user data
        "processed_by": None,  # Can be populated later
        "is_crypto_user": is_crypto_user,
        "smart_followers_count": None,  # Can be populated later
        "confidence_score": None,  # Can be populated later
    }


def insert_twitter_profiles_to_db(profiles_data):
    """Insert or update Twitter profiles data into twitter.twitter_profiles table"""
    if not profiles_data:
        return
    
    try:
        # Prepare data for batch insert/update
        values = []
        for profile_data in profiles_data:
            values.append((
                profile_data["author_id"],
                profile_data["name"],
                profile_data["handle"],
                profile_data["bio"],
                profile_data["url_in_bio"],
                profile_data["profile_image_url"],
                profile_data["profile_banner_url"],
                profile_data["followers_count"],
                profile_data["followings_count"],
                profile_data["is_verified"],
                profile_data["account_created_at"],
                profile_data["tag"],
                profile_data["is_active"],
                profile_data["inserted_at"],
                profile_data["updated_at"],
                profile_data["ai_tag"],
                profile_data["is_processed_by_ai"],
                profile_data["scraped_by"],
                profile_data["professional_category"],
                profile_data["lifetime_tweets"],
                profile_data["lifetime_views"],
                profile_data["processed_by"],
                profile_data["is_crypto_user"],
                profile_data["smart_followers_count"],
                profile_data["confidence_score"]
            ))
        
        # Execute batch insert/update (ON DUPLICATE KEY UPDATE handles both cases)
        db.executemany_query(INSERT_INTO_TWITTER_PROFILES_QUERY, values)
        print(f"✅ Successfully inserted/updated {len(profiles_data)} Twitter profiles into database")
        
    except Exception as e:
        print(f"❌ Error inserting/updating Twitter profiles to database: {e}")


def insert_profile_to_db(user_data, script_type="test_scraper"):
    """Insert or update a single Twitter profile into twitter.twitter_profiles table"""
    if not user_data:
        return
    
    try:
        # Prepare data for single insert/update
        values = (
            user_data["user_id"],
            user_data["username"],
            user_data["display_name"],
            user_data["bio"],
            user_data["location"],
            user_data["url"],
            user_data["verified"],
            user_data["followers_count"],
            user_data["following_count"],
            user_data["tweet_count"],
            user_data["listed_count"],
            user_data["created_at"],
            user_data["profile_image_url"],
            user_data["banner_url"],
            user_data["pinned_tweet_id"],
            user_data["tag"],
            user_data["is_active"],
            user_data["inserted_at"],
            user_data["updated_at"],
            user_data["ai_tag"],
            user_data["is_processed_by_ai"],
            user_data["scraped_by"],
            user_data["professional_category"],
            user_data["lifetime_tweets"],
            user_data["lifetime_views"],
            user_data["processed_by"],
            user_data["is_crypto_user"],
            user_data["smart_followers_count"],
            user_data["confidence_score"]
        )
        
        # Execute single insert/update (ON DUPLICATE KEY UPDATE handles both cases)
        db.execute_query(INSERT_INTO_TWITTER_PROFILES_QUERY, values)
        print(f"✅ Successfully inserted/updated Twitter profile {user_data['username']} into database")
        
    except Exception as e:
        print(f"❌ Error inserting/updating Twitter profile to database: {e}")
