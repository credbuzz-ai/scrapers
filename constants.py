INSERT_INTO_ENHANCED_TWEETS_QUERY = """
INSERT INTO twitter.enhanced_tweets 
(tweet_id, author_id, body, author_handle, tweet_create_time, create_time, retweet_count, 
 like_count, reply_count, quote_count, view_count, update_time, profile_image_url, 
 is_hidden, impressions, matching_values, is_mapped, sentiment, source, scraped_by,
 number_of_cashtags, tweet_category, first_cashtag, main_cashtag, number_of_hashtags,
 number_of_mentions, number_of_contracts, number_of_links, is_full_body, sentiment_new,
 is_reply, reply_to, is_quote, quoted_to, images)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
body=VALUES(body), retweet_count=VALUES(retweet_count), like_count=VALUES(like_count),
reply_count=VALUES(reply_count), quote_count=VALUES(quote_count), view_count=VALUES(view_count),
update_time=VALUES(update_time), impressions=VALUES(impressions)
"""

INSERT_INTO_TWITTER_PROFILES_QUERY = """
INSERT INTO twitter.twitter_profiles 
(author_id, name, handle, bio, url_in_bio, profile_image_url, profile_banner_url, 
 followers_count, followings_count, is_verified, account_created_at, tag, is_active, 
 inserted_at, updated_at, ai_tag, is_processed_by_ai, scraped_by, professional_category,
 lifetime_tweets, lifetime_views, processed_by, is_crypto_user, smart_followers_count, 
 confidence_score)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
name=VALUES(name), bio=VALUES(bio), url_in_bio=VALUES(url_in_bio), 
profile_image_url=VALUES(profile_image_url), profile_banner_url=VALUES(profile_banner_url), 
followers_count=VALUES(followers_count), followings_count=VALUES(followings_count), 
is_verified=VALUES(is_verified), account_created_at=VALUES(account_created_at),
updated_at=VALUES(updated_at), lifetime_tweets=VALUES(lifetime_tweets), 
lifetime_views=VALUES(lifetime_views), scraped_by=VALUES(scraped_by)
"""