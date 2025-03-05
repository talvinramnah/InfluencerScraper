import streamlit as st
import openai
from apify_client import ApifyClient
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
import numpy as np
import json

# ---------------------------------------------------
# 1. CONFIGURATIONS & SETUP
# ---------------------------------------------------

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# Set up your OpenAI API key from secrets
openai.api_key = st.secrets["openai"]["api_key"]

# Apify token from secrets
APIFY_API_TOKEN = st.secrets["apify"]["api_token"]

# Google Sheets Setup
SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
service_account_info = json.loads(st.secrets["google"]["service_account"])
CREDS = ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, SCOPE)
gc = gspread.authorize(CREDS)

# Replace with your actual spreadsheet name or key
SPREADSHEET_NAME = "UK Curriculum Influencers"

try:
    sh = gc.open(SPREADSHEET_NAME)
    logging.info(f"Successfully opened Google Sheet: {SPREADSHEET_NAME}")
except Exception as e:
    logging.error(f"Error opening Google Sheet: {e}")
    raise e

# Main worksheet for influencer data
try:
    main_worksheet = sh.worksheet("Main")
    logging.info("Main worksheet found.")
except Exception:
    main_worksheet = sh.add_worksheet(title="Main", rows=1000, cols=20)
    header = [
        "Profile Pic URL", "Username", "Posts Count", "Followers Count", 
        "Biography", "Profile Link",
        "Median Comments (last 5)", "Median Likes (last 5)", "Engagement Rate"
    ]
    main_worksheet.insert_row(header, 1)
    logging.info("Main worksheet created with header row.")

# Worksheet for hashtags
try:
    hashtag_worksheet = sh.worksheet("Hashtags")
    logging.info("Hashtags worksheet found.")
except Exception:
    hashtag_worksheet = sh.add_worksheet(title="Hashtags", rows=1000, cols=10)
    hashtag_header = ["Timestamp", "Input Hashtags", "Used Hashtags"]
    hashtag_worksheet.insert_row(hashtag_header, 1)
    logging.info("Hashtags worksheet created with header row.")

# ---------------------------------------------------
# 2. HELPER FUNCTIONS
# ---------------------------------------------------

# ----- Instagram Functions -----
def fetch_owner_usernames_from_hashtags_instagram(hashtags: list, results_limit: int) -> set:
    """
    For Instagram, use the Apify hashtag scraper (actor ID: reGe1ST3OBgYZSsZJ)
    to gather owner usernames.
    """
    client = ApifyClient(APIFY_API_TOKEN)
    unique_usernames = set()
    for htag in hashtags:
        logging.info(f"Instagram: Scraping hashtag: {htag}")
        try:
            run_input = {
                "hashtags": [htag],
                "resultsLimit": results_limit
            }
            # Using the Instagram hashtag scraper actor
            run = client.actor("reGe1ST3OBgYZSsZJ").call(run_input=run_input)
            for item in client.dataset(run["defaultDatasetId"]).iterate_items():
                if "ownerUsername" in item:
                    unique_usernames.add(item["ownerUsername"])
        except Exception as e:
            logging.error(f"Error scraping Instagram hashtag {htag}: {e}")
    logging.info(f"Instagram: Total unique usernames found: {len(unique_usernames)}")
    return unique_usernames

def scrape_profile_info_instagram(username: str):
    """
    Scrape Instagram profile info using Apify (actor ID: dSCLg0C3YEZ83HzYX) and return profile data.
    """
    client = ApifyClient(APIFY_API_TOKEN)
    try:
        run_input = {"usernames": [username]}
        run = client.actor("dSCLg0C3YEZ83HzYX").call(run_input=run_input)
        data_items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        if not data_items:
            logging.warning(f"Instagram: No profile data returned for {username}")
            return None
        profile_data = data_items[0]
        return {
            "username": username,
            "profile_pic_url": profile_data.get("profilePicUrl", ""),
            "posts_count": profile_data.get("postsCount", 0),
            "followers_count": profile_data.get("followersCount", 0),
            "biography": profile_data.get("biography", "")
        }
    except Exception as e:
        logging.error(f"Instagram: Error scraping profile info for {username}: {e}")
        return None

def get_last_5_posts_stats_instagram(username: str, limit: int = 30):
    """
    For Instagram, use Apify (actor ID: nH2AHrwxeTRJoN5hX) to get recent posts and return median likes/comments.
    """
    client = ApifyClient(APIFY_API_TOKEN)
    try:
        run_input = {"username": [username], "resultsLimit": limit}
        run = client.actor("nH2AHrwxeTRJoN5hX").call(run_input=run_input)
        posts = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        if not posts:
            logging.warning(f"Instagram: No posts found for {username}")
            return 0, 0
        posts.sort(key=lambda x: x.get("takenAtTimestamp", 0), reverse=True)
        recent_posts = posts[:5]
        likes_list = [p.get("likesCount", 0) for p in recent_posts]
        comments_list = [p.get("commentsCount", 0) for p in recent_posts]
        if not likes_list:
            return 0, 0
        median_likes = int(np.median(likes_list))
        median_comments = int(np.median(comments_list))
        return median_likes, median_comments
    except Exception as e:
        logging.error(f"Instagram: Error scraping posts for {username}: {e}")
        return 0, 0

# ----- TikTok Functions -----
def fetch_owner_usernames_from_hashtags_tiktok(hashtags: list, results_per_page: int) -> (set, dict):
    """
    For TikTok, use the hashtag scraper actor (actor ID: f1ZeP0K58iwlqG2pY).
    Returns a set of unique usernames (from authorMeta.nickName) and a dictionary mapping each username to a list of post items.
    """
    client = ApifyClient(APIFY_API_TOKEN)
    unique_usernames = set()
    posts_by_user = {}
    for htag in hashtags:
        logging.info(f"TikTok: Scraping hashtag: {htag}")
        try:
            run_input = {
                "hashtags": [htag],
                "resultsPerPage": results_per_page,
                "shouldDownloadVideos": False,
                "shouldDownloadCovers": False,
                "shouldDownloadSubtitles": False,
                "shouldDownloadSlideshowImages": False,
            }
            run = client.actor("f1ZeP0K58iwlqG2pY").call(run_input=run_input)
            for item in client.dataset(run["defaultDatasetId"]).iterate_items():
                # Extract username from authorMeta.nickName
                try:
                    username = item["authorMeta"]["nickName"]
                except Exception:
                    continue
                unique_usernames.add(username)
                posts_by_user.setdefault(username, []).append(item)
        except Exception as e:
            logging.error(f"TikTok: Error scraping hashtag {htag}: {e}")
    logging.info(f"TikTok: Total unique usernames found: {len(unique_usernames)}")
    return unique_usernames, posts_by_user

def scrape_profile_info_tiktok(username: str):
    """
    For TikTok, use the profile scraper actor (actor ID: 0FXVyOXXEmdGcV88a) to get profile info.
    """
    client = ApifyClient(APIFY_API_TOKEN)
    try:
        run_input = {
            "profiles": [username],
            "profileScrapeSections": ["videos"],
            "profileSorting": "latest",
            "resultsPerPage": 100,
            "excludePinnedPosts": False,
            "shouldDownloadVideos": False,
            "shouldDownloadCovers": False,
            "shouldDownloadSubtitles": False,
            "shouldDownloadSlideshowImages": False,
            "shouldDownloadAvatars": False,
        }
        run = client.actor("0FXVyOXXEmdGcV88a").call(run_input=run_input)
        data_items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        if not data_items:
            logging.warning(f"TikTok: No profile data returned for {username}")
            return None
        profile_data = data_items[0]
        author = profile_data.get("authorMeta", {})
        return {
            "username": author.get("nickName", username),
            "profile_pic_url": author.get("avatar", ""),
            "posts_count": author.get("video", 0),
            "followers_count": author.get("fans", 0),
            "biography": author.get("signature", "")
        }
    except Exception as e:
        logging.error(f"TikTok: Error scraping profile info for {username}: {e}")
        return None

def get_last_5_posts_stats_tiktok(username: str, posts_by_user: dict):
    """
    For TikTok, use the posts already collected from the hashtag scraper.
    Filter posts by the given username, sort by creation time, then compute median of diggCount (likes) and commentCount.
    """
    user_posts = posts_by_user.get(username, [])
    if not user_posts:
        logging.warning(f"TikTok: No posts found for {username} in hashtag scrape.")
        return 0, 0
    # Sort posts by createTime (assuming 'createTime' exists)
    user_posts.sort(key=lambda x: x.get("createTime", 0), reverse=True)
    recent_posts = user_posts[:5]
    likes_list = [int(post.get("diggCount", 0)) for post in recent_posts]
    comments_list = [int(post.get("commentCount", 0)) for post in recent_posts]
    if not likes_list:
        return 0, 0
    median_likes = int(np.median(likes_list))
    median_comments = int(np.median(comments_list))
    return median_likes, median_comments

def append_profile_to_sheet(profile_data: dict, median_comments: int, median_likes: int, engagement_rate: float):
    """
    Append the qualifying profile data along with engagement metrics to the main worksheet.
    """
    row = [
        profile_data["profile_pic_url"],
        profile_data["username"],
        profile_data["posts_count"],
        profile_data["followers_count"],
        profile_data["biography"],
        f"https://www.tiktok.com/@{profile_data['username']}",
        str(median_comments),
        str(median_likes),
        f"{engagement_rate:.2f}"
    ]
    main_worksheet.append_row(row)
    logging.info(f"Stored profile data for {profile_data['username']}")

def append_profile_to_sheet_instagram(profile_data: dict, median_comments: int, median_likes: int, engagement_rate: float):
    """
    Append Instagram profile data to the sheet.
    """
    row = [
        profile_data["profile_pic_url"],
        profile_data["username"],
        profile_data["posts_count"],
        profile_data["followers_count"],
        profile_data["biography"],
        f"https://www.instagram.com/{profile_data['username']}",
        str(median_comments),
        str(median_likes),
        f"{engagement_rate:.2f}"
    ]
    main_worksheet.append_row(row)
    logging.info(f"Stored Instagram profile data for {profile_data['username']}")

def append_hashtags_to_sheet(input_str: str, hashtags: list):
    """
    Store the entered hashtags in the 'Hashtags' worksheet.
    """
    from datetime import datetime
    try:
        hashtags_str = ", ".join(hashtags)
        row = [datetime.now().strftime("%Y-%m-%d %H:%M:%S"), input_str, hashtags_str]
        hashtag_worksheet.append_row(row)
        logging.info("Hashtags appended to 'Hashtags' worksheet.")
    except Exception as e:
        logging.error(f"Error appending hashtags to sheet: {e}")

# ---------------------------------------------------
# 3. STREAMLIT APP
# ---------------------------------------------------
def main():
    st.title("Influencer Sourcing Automation")
    
    # Platform toggle: Instagram or TikTok
    platform = st.selectbox("Select Platform", options=["Instagram", "TikTok"])
    
    st.write(
        "Please enter comma-separated hashtags (e.g. #InternationalBaccalaureate, #IBExams, #IBDiploma):"
    )
    hashtags_input = st.text_input("Hashtags", "")
    
    # For Instagram, use resultsLimit; for TikTok, use resultsPerPage.
    results_input = st.number_input("How many posts per hashtag to scrape?", min_value=1, max_value=1000, value=50)
    
    if st.button("Scrape Influencers"):
        if not hashtags_input.strip():
            st.error("Please enter at least one hashtag.")
            return
        
        # Parse hashtags from input
        hashtags = [tag.strip() for tag in hashtags_input.split(",") if tag.strip()]
        if not hashtags:
            st.error("No valid hashtags entered.")
            return
        
        st.success(f"Using {len(hashtags)} hashtags: {', '.join(hashtags)}")
        append_hashtags_to_sheet(hashtags_input, hashtags)
        
        # Define filtering criteria (both platforms):
        # - Followers > 1000
        # - Posts > 20
        # - Engagement rate >= 0.25%
        min_followers = 1000
        min_posts = 20
        min_engagement = 0.25
        
        if platform == "Instagram":
            # Instagram flow
            unique_usernames = fetch_owner_usernames_from_hashtags_instagram(hashtags, results_input)
            for username in unique_usernames:
                if username and user_already_in_sheet(username):
                    logging.info(f"Instagram: Skipping {username}, already in sheet.")
                    continue
                profile_data = scrape_profile_info_instagram(username)
                if profile_data is None:
                    continue
                if profile_data["followers_count"] > min_followers and profile_data["posts_count"] > min_posts:
                    median_likes, median_comments = get_last_5_posts_stats_instagram(username, limit=results_input)
                    if profile_data["followers_count"] > 0:
                        engagement_rate = ((median_likes + median_comments) / profile_data["followers_count"]) * 100
                    else:
                        engagement_rate = 0
                    if engagement_rate < min_engagement:
                        logging.info(f"Instagram: Skipping {username} due to low engagement rate: {engagement_rate:.2f}%")
                        continue
                    append_profile_to_sheet_instagram(profile_data, median_comments, median_likes, engagement_rate)
                    
        elif platform == "TikTok":
            # TikTok flow
            unique_usernames, posts_by_user = fetch_owner_usernames_from_hashtags_tiktok(hashtags, results_input)
            for username in unique_usernames:
                if username and user_already_in_sheet(username):
                    logging.info(f"TikTok: Skipping {username}, already in sheet.")
                    continue
                profile_data = scrape_profile_info_tiktok(username)
                if profile_data is None:
                    continue
                if profile_data["followers_count"] > min_followers and profile_data["posts_count"] > min_posts:
                    median_likes, median_comments = get_last_5_posts_stats_tiktok(username, posts_by_user)
                    if profile_data["followers_count"] > 0:
                        engagement_rate = ((median_likes + median_comments) / profile_data["followers_count"]) * 100
                    else:
                        engagement_rate = 0
                    if engagement_rate < min_engagement:
                        logging.info(f"TikTok: Skipping {username} due to low engagement rate: {engagement_rate:.2f}%")
                        continue
                    append_profile_to_sheet(profile_data, median_comments, median_likes, engagement_rate)
        
        st.success("Scraping and data append complete. Please check Google Sheets for results.")

if __name__ == "__main__":
    main()
