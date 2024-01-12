import os
import time
import logging
import instaloader
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
from PIL import Image
from pytz import timezone

load_dotenv()  # take environment variables from .env.

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Exception as e:
        print(f"The error '{e}' occurred")


def update_last_fetched(target_profile, connection):
    current_time = datetime.now()
    query = f"""INSERT INTO insta_to_reddit_last_fetched (profile_name, last_fetched) VALUES ('{target_profile}', '{current_time}')"""
    execute_query(connection, query)


def get_last_fetched(connection, profile_name):
    cursor = connection.cursor()
    try:
        query = f"SELECT last_fetched FROM insta_to_reddit_last_fetched WHERE profile_name = '{profile_name}' ORDER BY last_fetched DESC LIMIT 1"
        cursor.execute(query)
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            print(f"No 'last_fetched' value found for profile '{profile_name}'")
    except Exception as e:
        print(f"The error '{e}' occurred")


def create_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)
        logging.info(f"Created directory: {path}")


def download_post(post, post_folder_path):
    # Configure Instaloader
    L = instaloader.Instaloader()
    # Set the download delay to avoid hitting rate limits
    L.download_delay = 10  # or another suitable value
    # Set directory pattern
    L.dirname_pattern = post_folder_path

    # Attempt to download the post
    try:
        L.download_post(post, target=post_folder_path)  # Modified line
        with open(os.path.join(post_folder_path, 'caption.txt'), 'w', encoding='utf-8') as file:
            file.write(post.caption or '')
            file.write(f"\nSource: {post.url}")
        print(f"Downloaded post from {post.date_local}")
        sidecar_nodes = list(post.get_sidecar_nodes())
        if len(sidecar_nodes) > 1 and len(sidecar_nodes) <= 3:
            images = []
            for node in post.get_sidecar_nodes():
                image_path = os.path.join(post_folder_path, f"{post.date_local.strftime('%Y-%m-%d_%H-%M-%S')}.jpg")
                L.download_post(node, target=image_path)  # Modified line
                images.append(Image.open(image_path))

            widths, heights = zip(*(i.size for i in images))
            total_height = sum(heights)
            max_width = max(widths)

            new_image = Image.new('RGB', (max_width, total_height))

            y_offset = 0
            for image in images:
                new_image.paste(image, (0, y_offset))
                y_offset += image.size[1]

            new_image.save(os.path.join(post_folder_path, 'combined_images.jpg'))

    except instaloader.exceptions.ConnectionException:  # Modified line
        print("Rate limit hit, sleeping for 10 minutes")
        time.sleep(600)  # Sleep for 10 minutes

    except Exception as e:
        print(f"An error occurred: {e}")


def download_recent_posts(target_profile):
    # Configure Instaloader
    L = instaloader.Instaloader()
    # Define root directory for storing posts
    create_directory(target_profile)

    connection = create_connection(
        db_name=os.getenv('DB_NAME'),
        db_user=os.getenv('DB_USER'),
        db_password=os.getenv('DB_PASSWORD'),
        db_host=os.getenv('DB_HOST'),
        db_port=os.getenv('DB_PORT')
    )

    last_checked = get_last_fetched(connection, target_profile)
    print(f"Last checked: {last_checked}")
    two_days_before_now = datetime.now() - timedelta(days=10)
    if last_checked is None:
        earliest_date_to_check = two_days_before_now
    else:
        earliest_date_to_check = max(last_checked, two_days_before_now)

    print("Starting download...")
    profile = instaloader.Profile.from_username(L.context, target_profile)
    for post in profile.get_posts():
        # Make both datetimes timezone-naive
        post_date_naive = post.date_local.replace(tzinfo=None)
        earliest_date_naive = earliest_date_to_check.replace(tzinfo=None)

        if post_date_naive <= earliest_date_naive:
            print(f"Post on {post_date_naive} is older than the threshold, stopping.")
            break

        download_post_for_profile(post, target_profile)

    update_last_fetched(target_profile, connection)
    print("All recent posts downloaded successfully.")


def download_post_for_profile(post, target_profile):
    date_str = post.date_local.replace(tzinfo=None).strftime('%Y-%m-%d_%H-%M-%S')
    folder_name = f"{date_str}"
    post_folder_path = os.path.join(target_profile, folder_name)

    create_directory(post_folder_path)
    download_post(post, post_folder_path)


if __name__ == '__main__':
    download_recent_posts('whats_happening_in_tvm')