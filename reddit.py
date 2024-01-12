import os
import shutil
import praw
import logging
import os

from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

# Setup basic configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize logging
logger = logging.getLogger(__name__)

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

def move_folder_to_archived(source_folder):
    archive_folder = 'whats_happening_in_tvm/archived'
    try:
        shutil.move(source_folder, archive_folder)
        logger.info(f"Moved {source_folder} to {archive_folder}.")
    except Exception as e:
        logger.error(f"Error in moving folder to archived: {e}")

def check_files_and_post(reddit, folder_path, subreddit_name, flair_text):
    try:
        jpg_files = [f for f in os.listdir(folder_path) if f.endswith('.jpg')]
        mp4_files = [f for f in os.listdir(folder_path) if f.endswith('.mp4')]

        # Move folder to archived if conditions are not met
        if len(jpg_files) != 1 or mp4_files:
            move_folder_to_archived(folder_path)
            logger.info(f"Folder {folder_path} moved to archived due to file conditions.")
            return

        # Posting to Reddit
        image_path = os.path.join(folder_path, jpg_files[0])
        with open(os.path.join(folder_path, 'caption.txt'), 'r', encoding='utf-8') as file:
            caption = file.readlines()
        title = caption[0].strip()
        comment = ''.join(caption).strip()

        # Post to Reddit
        logger.info(f"Posting to Reddit from folder {folder_path}.")
        post_to_reddit(reddit, image_path, title, comment, subreddit_name, flair_text)
    except Exception as e:
        logger.error(f"Error in check_files_and_post for folder {folder_path}: {e}")

def find_flair_id(subreddit, flair_text):
    try:
        flairs = subreddit.flair.link_templates
        for flair in flairs:
            if flair['text'].lower() == flair_text.lower():
                return flair['id']
        return None
    except Exception as e:
        logger.error(f"Error in find_flair_id: {e}")
        return None

def post_to_reddit(reddit, image_path, title, comment, subreddit_name, flair_text):
    try:
        subreddit = reddit.subreddit(subreddit_name)
        flair_id = find_flair_id(subreddit, flair_text)

        if flair_id:
            submission = subreddit.submit_image(title, image_path, flair_id=flair_id)
        else:
            logger.warning(f"No matching flair for text '{flair_text}'. Submitting without flair.")
            submission = subreddit.submit_image(title, image_path)

        submission.reply(comment)
        logger.info(f"Posted to subreddit {subreddit_name} with title {title}.")
    except Exception as e:
        logger.error(f"Error in post_to_reddit: {e}")

# Main function to iterate through folders
def main(base_folder, subreddit_name, flair_text):
    try:
        reddit = get_reddit_api_token()
        logger.info(f"Starting to process folders in {base_folder}.")

        for folder in os.listdir(base_folder):
            # Skip the 'archived' folder
            if folder == 'archived':
                logger.info("Skipped 'archived' folder.")
                continue

            folder_path = os.path.join(base_folder, folder)
            if os.path.isdir(folder_path):
                logger.info(f"Processing folder {folder_path}.")
                check_files_and_post(reddit, folder_path, subreddit_name, flair_text)
        logger.info("Finished processing all folders.")
    except Exception as e:
        logger.error(f"Error in main function: {e}")

if __name__ == "__main__":
    base_folder = 'whats_happening_in_tvm'
    subreddit_name = 'Trivandrum'  # Replace with your subreddit
    flair_text = 'Events'  # Replace with your flair text
    main(base_folder, subreddit_name, flair_text)
