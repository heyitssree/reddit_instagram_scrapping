import os
import string
import time
import threading
from datetime import datetime, timezone
import logging
import requests
import concurrent.futures
import praw
from dotenv import load_dotenv
import psycopg2


from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

# Setup basic configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize logging
logger = logging.getLogger(__name__)

def create_connection(db_name, db_user, db_password, db_host, db_port):
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        logging.info("Connection to PostgreSQL DB successful")
        return connection
    except Exception as e:
        logging.error(f"The error '{e}' occurred")
        return None

def check_if_data_exists(connection, table_name, column_name, value):
    """Returns True if the given value exists in the given column of the given table."""
    cursor = connection.cursor()
    try:
        query = f"SELECT * FROM {table_name} WHERE {column_name} = '{value}'"
        cursor.execute(query)
        result = cursor.fetchone()
        if result:
            return True
        else:
            return False
    except Exception as e:
        logger.info(f"The error '{e}' occurred")

def insert_into_db(connection, table_name: str, columns: list, values: list):
    """Inserts the given values into the given table."""
    try:
        cursor = connection.cursor()
        columns_str = ', '.join(columns)
        values_str = ', '.join([f"'{value}'" for value in values])
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})"
        cursor.execute(query)
        connection.commit()  # Commit the transaction
        logger.info("Query executed successfully")
    except Exception as e:
        logger.info(f"The error '{e}' occurred")
        logger.info(f"Failed to execute query: {query}")

def get_reddit_api_token():
    logger.info("Retrieving Reddit API token.")
    try:
        client_id = os.environ.get('REDDIT_CLIENT_ID')
        client_secret = os.environ.get('REDDIT_CLIENT_SECRET')
        user_agent = os.environ.get('REDDIT_USER_AGENT')
        username = os.environ.get('REDDIT_USERNAME')
        password = os.environ.get('REDDIT_PASSWORD')

        if not all([client_id, client_secret, user_agent, username, password]):
            raise ValueError("Missing required environment variables")

        reddit = praw.Reddit(client_id=client_id,
                             client_secret=client_secret,
                             user_agent=user_agent,
                             username=username,
                             password=password)
        logger.info("Reddit API token retrieved successfully.")
        return reddit
    except Exception as e:
        logger.error(f"Error in get_reddit_api_token: {e}")
        raise

def sanitize_title(title):
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
    sanitized = ''.join(c for c in title if c in valid_chars)
    return sanitized.replace(" ", "_").replace("(", "").replace(")", "")

def download_image(url, path, total, counter_lock, completed_counter):
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(path, 'wb') as f:
                for chunk in response.iter_content(8192):
                    f.write(chunk)
            with counter_lock:
                completed_counter[0] += 1
                logger.info(f"Downloaded image {completed_counter[0]}/{total}: {path}", flush=True)
    except Exception as e:
        logger.info(f"An error occurred while downloading {url}: {e}", flush=True)

def format_datetime(timestamp):
    return datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y%m%d_%H%M%S')

def download_images_from_subreddit(subreddit_name, search_title, connection):
    start_time = time.time()
    reddit = get_reddit_api_token()  # Ensure this function is defined or replace with your Reddit instance
    max_images=1000
    directory_name = os.path.join('images', subreddit_name, search_title)
    if not os.path.exists(directory_name):
        os.makedirs(directory_name)

    images_downloaded = 0
    last_post = None

    while images_downloaded < max_images:
        subreddit = reddit.subreddit(subreddit_name)
        posts = subreddit.search(search_title, limit=100, params={"after": last_post})

        images = []
        for post in posts:
            last_post = post.fullname  # Store the fullname of the last post
            if search_title.lower() in post.title.lower() and post.url.endswith(('.jpg', '.jpeg', '.png', '.gif')):
                sanitized_title = sanitize_title(post.title)
                file_extension = os.path.splitext(post.url)[1]
                datetime_string = format_datetime(post.created_utc)
                image_path = os.path.join(directory_name, f"{sanitized_title}_{datetime_string}{file_extension}")
                images.append((post.url, image_path, post.title))  # Include post title in the images list

        if not images:
            break  # No more posts to process

        total_images = len(images)
        completed_counter = [0]
        counter_lock = threading.Lock()

        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            for url, path, title in images:  # Unpack the post title from the images list
                if check_if_data_exists(connection=connection, table_name='image_download_history', column_name='post_url', value=url):
                    logger.info(f"Skipping {url} as it has already been downloaded.")
                    continue
                executor.submit(download_image, url, path, total_images, counter_lock, completed_counter)
                images_downloaded += 1
                if images_downloaded >= max_images:
                    break

                insert_into_db(connection=connection, table_name='image_download_history', columns=('search_term', 'post_title', 'subreddit_name', 'post_url', 'download_time'), values=(search_title, title, subreddit_name, url, datetime.now()))  # Use the corresponding post title

    end_time = time.time()
    logger.info(f"Total execution time: {end_time - start_time:.2f} seconds")
    logger.info(f"Total images downloaded: {images_downloaded}")


connection = create_connection(
    db_name=os.getenv('DB_NAME'),
    db_user=os.getenv('DB_USER'),
    db_password=os.getenv('DB_PASSWORD'),
    db_host=os.getenv('DB_HOST'),
    db_port=os.getenv('DB_PORT')
)

user_subreddits = input("Enter subreddit names, separated by commas: ")
search_term = input("Enter the search terms, separated by comma: ")

subreddit_list = [sub.strip() for sub in user_subreddits.split(',')]  # Split and strip subreddit names
search_list = [search.strip() for search in search_term.split(',')]  # Split and strip search terms

for subreddit in subreddit_list:
    download_images_from_subreddit(subreddit_name=subreddit, search_title=search_term, connection=connection)

connection.close()