import os 
import re
import time
import json 
import logging 
import pandas as pd
import requests as r
import multiprocessing
import contextlib
import joblib
from tqdm import tqdm
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from farcaster import Warpcast

from ..helpers.scraper import Scraper
from ...helpers.multiprocessing import Multiprocessing  # Import Multiprocessing from the proper module
from .cyphers import FcsScraperCyphers

# Force reload environment variables
load_dotenv(override=True)

class FcsScraper(Scraper):
    def __init__(self, bucket_name="fcs", load_data=False, weeks=1):
        super().__init__(bucket_name=bucket_name, load_data=load_data)
        self.cyphers = FcsScraperCyphers()
        self.FARCASTER_EPOCH = datetime(2021, 1, 1, tzinfo=timezone.utc)
        self.NEYNAR_API_KEY = os.getenv('NEYNAR_API_KEY')
        
        # Initialize cutoff_timestamp for time filtering
        self.cutoff_timestamp = None
        if weeks is not None:
            current_time = datetime.now(timezone.utc)
            self.cutoff_timestamp = (current_time - timedelta(days=7 * weeks)).timestamp()
            self.cutoff_datetime_str = (current_time - timedelta(days=7 * weeks)).strftime("%Y-%m-%dT%H:%M:%SZ")
            logging.info(f"Using cutoff timestamp: {self.cutoff_timestamp} ({datetime.fromtimestamp(self.cutoff_timestamp, tz=timezone.utc)})")
            logging.info(f"Cutoff datetime string: {self.cutoff_datetime_str}")
            
        # Rate limiting for Neynar API
        self.request_timestamps = []
        self.max_requests_per_minute = 500  # Set to 500 requests per minute
            
        # Initialize multiprocessing
        self.mp = Multiprocessing()  # Use the imported class

        self.farcaster_client = Warpcast(mnemonic=os.getenv('MNEMONIC')) 

    
    def apply_rate_limit(self):
        """
        Implement rate limiting to stay within 500 requests per 60s window for Neynar APIs
        """
        current_time = time.time()
        
        # Remove timestamps older than 60 seconds
        self.request_timestamps = [ts for ts in self.request_timestamps if current_time - ts < 60]
        
        # If we've reached the limit, wait until we can make another request
        if len(self.request_timestamps) >= self.max_requests_per_minute:
            oldest_timestamp = min(self.request_timestamps)
            sleep_time = 60 - (current_time - oldest_timestamp)
            if sleep_time > 0:
                logging.info(f"Rate limit reached ({self.max_requests_per_minute} requests/minute). Sleeping for {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
        
        # Add current request timestamp
        self.request_timestamps.append(time.time())

    def apply_user_rate_limit(self):
        """Simple 0.1 second rate limit for get_user calls"""
        time.sleep(0.1)  # 100ms delay between user requests
        return

    # Your existing methods remain the same...
    def convert_timestamp(self, timestamp):
        """Convert Farcaster timestamp to UTC datetime."""
        dt = self.FARCASTER_EPOCH + timedelta(seconds=int(timestamp))
        return dt.timestamp()  # Return Unix timestamp as float

    def parse_iso_timestamp(self, timestamp_str):
        """Parse ISO format timestamp string to Unix timestamp."""
        try:
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return dt.timestamp()
        except Exception as e:
            logging.error(f"Error parsing timestamp {timestamp_str}: {e}")
            return None

    def is_after_cutoff(self, timestamp, is_unix=True):
        """
        Check if a timestamp is after the cutoff timestamp.
        
        Args:
            timestamp: Either a Unix timestamp (float/int) or ISO format string
            is_unix: Whether the timestamp is already in Unix format
            
        Returns:
            bool: True if after cutoff or no cutoff set, False otherwise
        """
        if self.cutoff_timestamp is None:
            return True  # No cutoff, include everything
            
        if is_unix:
            # Direct comparison for Unix timestamps
            unix_timestamp = timestamp
        else:
            # Parse string timestamp to Unix timestamp
            unix_timestamp = self.parse_iso_timestamp(timestamp)
            if unix_timestamp is None:
                return False  # Failed to parse, skip it
                
        return unix_timestamp >= self.cutoff_timestamp

    def query_neynar_hub(self, endpoint, params=None):
        """
        Fetch messages from the Neynar hub
        """
        base_url = "https://hub-api.neynar.com/v1/"
        headers = {
            "Content-Type": "application/json",
            "api_key": self.NEYNAR_API_KEY,
        }
        url = f"{base_url}{endpoint}"

        # Build params
        req_params = (params or {}).copy()
        req_params['pageSize'] = 100

        all_messages = []
        max_retries = 3
        initial_retry_delay = 1.0

        # === retry/backoff for this request ===
        retry_delay = initial_retry_delay
        for attempt in range(max_retries):
            try:
                # Apply rate limiting before making request
                self.apply_rate_limit()
                
                logging.debug(f"GET {url} params={req_params!r}")
                response = r.get(url, headers=headers, params=req_params)

                if response.status_code != 200:
                    # Special handling for rate limit errors (429)
                    if response.status_code == 429:
                        logging.warning(f"Rate limit exceeded (429). Sleeping for 45 seconds before retry...")
                        time.sleep(45)  # Sleep for 45 seconds on rate limit errors
                        continue  # Try the request again immediately after sleeping
                        
                    logging.error(f"Non-200 response: {response.status_code} – {response.text}")
                    return all_messages

                response.raise_for_status()
                data = response.json()
                break  # success, exit retry loop

            except Exception as e:
                if attempt == max_retries - 1:
                    logging.error(f"Failed after {max_retries} attempts: {e}")
                    return all_messages
                logging.warning(f"Attempt {attempt+1} failed ({e}), retrying in {retry_delay}s…")
                time.sleep(retry_delay)
                retry_delay *= 2

        # === collect and convert timestamps ===
        msgs = data.get('messages', [])
        filtered_messages = []
        
        for msg in msgs:
            ts = msg.get('data', {}).get('timestamp')
            if ts is not None:
                unix_ts = int(self.convert_timestamp(ts))
                msg['data']['timestamp'] = unix_ts
                
                # Apply cutoff timestamp filtering
                if self.is_after_cutoff(unix_ts):
                    filtered_messages.append(msg)
                    
        all_messages.extend(filtered_messages)
        logging.info(f"Retrieved {len(filtered_messages)} messages after timestamp filtering (from {len(msgs)} total)...")

        return all_messages
        
    def query_neynar_api(self, endpoint, params=None):
        """
        Fetch data from the Neynar API with ISO format timestamps
        """
        base_url = "https://api.neynar.com/v2/farcaster/"  # Added 'farcaster/' to the path
        headers = {
            "Content-Type": "application/json",
            "x-api-key": self.NEYNAR_API_KEY,  # Changed from 'api_key' to 'x-api-key'
        }
        url = f"{base_url}{endpoint}"

        # Build params
        req_params = (params or {}).copy()
        req_params['limit'] = 100  # API limit is 100 max

        all_data = []
        max_retries = 3
        initial_retry_delay = 1.0
        cursor = None
        consecutive_no_new_data = 0  # Counter for consecutive requests with no new data
        max_consecutive_no_new_data = 3  # Maximum allowed consecutive requests with no new data

        # Implement pagination with cursor
        while True:
            # Update params with cursor if available
            if cursor:
                req_params['cursor'] = cursor

            # === retry/backoff for this request ===
            retry_delay = initial_retry_delay
            success = False
            data = None
            
            for attempt in range(max_retries):
                try:
                    # Apply rate limiting before making request
                    self.apply_rate_limit()
                    
                    logging.debug(f"GET {url} params={req_params!r}")
                    response = r.get(url, headers=headers, params=req_params)

                    if response.status_code != 200:
                        # Special handling for rate limit errors (429)
                        if response.status_code == 429:
                            logging.warning(f"Rate limit exceeded (429). Sleeping for 45 seconds before retry...")
                            time.sleep(45)  # Sleep for 45 seconds on rate limit errors
                            continue  # Try the request again immediately after sleeping
                        
                        logging.error(f"Non-200 response: {response.status_code} – {response.text}")
                        if attempt == max_retries - 1:
                            return all_data
                        else:
                            time.sleep(retry_delay)
                            retry_delay *= 2
                            continue

                    response.raise_for_status()
                    data = response.json()
                    success = True
                    break  # success, exit retry loop

                except Exception as e:
                    if attempt == max_retries - 1:
                        logging.error(f"Failed after {max_retries} attempts: {e}")
                        return all_data
                    logging.warning(f"Attempt {attempt+1} failed ({e}), retrying in {retry_delay}s…")
                    time.sleep(retry_delay)
                    retry_delay *= 2

            if not success or not data:
                break

            # === filter data by timestamp ===
            reactions = data.get('reactions', [])  # The API returns a 'reactions' array
            if not reactions:
                break  # No more data to process
                
            filtered_items = []
            
            for reaction in reactions:
                timestamp_str = reaction.get('reaction_timestamp')
                if timestamp_str:
                    # Apply cutoff timestamp filtering using ISO format
                    if self.is_after_cutoff(timestamp_str, is_unix=False):
                        filtered_items.append(reaction)
            
            # Track size before adding new items
            previous_size = len(all_data)
            all_data.extend(filtered_items)
            current_size = len(all_data)
            
            # Check if we got any new data
            if current_size == previous_size:
                consecutive_no_new_data += 1
                logging.warning(f"No new data added after pagination. Consecutive: {consecutive_no_new_data}/{max_consecutive_no_new_data}")
                if consecutive_no_new_data >= max_consecutive_no_new_data:
                    logging.warning(f"Breaking pagination loop after {max_consecutive_no_new_data} consecutive requests with no new data")
                    break
            else:
                # Reset counter if we got new data
                consecutive_no_new_data = 0
            
            # Check for next cursor for pagination
            next_cursor = data.get('next', {}).get('cursor')
            if not next_cursor:
                break  # No more pages
                
            cursor = next_cursor
            logging.info(f"Retrieved {len(all_data)} reactions so far, fetching next page...")

        logging.info(f"Retrieved {len(all_data)} reactions after timestamp filtering...")
        return all_data

    def get_start_fid(self):
        last_fid = self.cyphers.get_last_fid()
        start_fid = last_fid + 1
        return start_fid
    
    def get_user(self, fid):
        user_obj = self.farcaster_client.get_user(fid)
        user_obj_dict = dict(user_obj)        
        # Access nested attributes properly
        user_dict = {
            'fid': user_obj_dict.get('fid'),
            'username': user_obj_dict.get('username'),
            'displayName': user_obj_dict.get('display_name'),
            'pfpUrl': user_obj.pfp.url if user_obj.pfp else None,  # Access as attribute, not dict
            'bio': user_obj.profile.bio.text if user_obj.profile and user_obj.profile.bio else None,
            'mentionedUsernames': user_obj.profile.bio.mentions if user_obj.profile and user_obj.profile.bio else [],
            'followerCount': user_obj_dict.get('follower_count'),
            'followingCount': user_obj_dict.get('following_count'),
        }
        return user_dict
    

    def fetch_users_until_end(self):
        """Get users by incrementing FID until 3 consecutive failures"""
        current_fid = self.get_start_fid()
        consecutive_failures = 0
        all_users = []
        
        logging.info(f"Starting to fetch users from FID {current_fid}")
        
        while consecutive_failures < 3:  # Stop after 3 consecutive failures
            try:
                self.apply_user_rate_limit()  # Use special rate limit for user requests
                user_dict = self.get_user(current_fid)
                
                if user_dict and user_dict.get('fid'):
                    consecutive_failures = 0
                    all_users.append(user_dict)
                else:
                    consecutive_failures += 1
            except Exception as e:
                consecutive_failures += 1
                logging.warning(f"Error for FID {current_fid}: {str(e)} - Failure {consecutive_failures}/3")
                time.sleep(consecutive_failures * 2)  # Backoff: 2s, 4s, 6s
            
            current_fid += 1
            
        logging.info(f"Fetched {len(all_users)} new users")
        return all_users

    def get_user_follows(self, fid):
        endpoint = "linksByFid"
        params = {
            'fid': fid, 
            'link_type': 'follow'
        }
        messages = self.query_neynar_hub(endpoint=endpoint, params=params)

        return [{
            'source': fid,
            'target': item['data']['linkBody'].get('targetFid'),
            'timestamp': item['data'].get('timestamp'),
        } for item in messages 
          if "data" in item 
          and "linkBody" in item["data"] 
          and item['data']['linkBody'].get('targetFid') 
          and item['data'].get('timestamp')]

    def get_user_likes(self, fid):
        endpoint = "reactions/user"
        params = {
            'fid': fid,
            'type': 'likes'  # Use 'likes' as the type parameter
        }
        reactions = self.query_neynar_api(endpoint, params)

        return [{
            'source': fid,
            'target': reaction['cast']['author']['fid'],
            'timestamp': datetime.fromisoformat(reaction['reaction_timestamp'].replace('Z', '+00:00')).timestamp()
        } for reaction in reactions 
          if reaction.get('cast')
          and reaction.get('cast', {}).get('author', {}).get('fid')
          and reaction.get('reaction_timestamp')]

    def get_user_recasts(self, fid):
        endpoint = "reactions/user"
        params = {
            'fid': fid,
            'type': 'recasts'  # Use 'recasts' as the type parameter
        }
        reactions = self.query_neynar_api(endpoint, params)

        return [{
            'source': fid,
            'target': reaction['cast']['author']['fid'],
            'target_hash': reaction['cast']['hash'],
            'timestamp': datetime.fromisoformat(reaction['reaction_timestamp'].replace('Z', '+00:00')).timestamp()
        } for reaction in reactions 
          if reaction.get('cast')
          and reaction.get('cast', {}).get('author', {}).get('fid')
          and reaction.get('reaction_timestamp')]

    def get_user_casts(self, fid):
        logging.info(f"Collecting casts for user {fid}.....")
        endpoint = "castsByFid"
        params = {'fid': fid}
        messages = self.query_neynar_hub(endpoint=endpoint, params=params)

        cast_data_list = [{
            'source': fid,
            'target': message['data']['castAddBody']['parentCastId']['fid'],
            'timestamp': message['data']['timestamp'],
        } for message in messages 
          if 'data' in message 
          and 'castAddBody' in message['data'] 
          and message['data']['castAddBody'].get('parentCastId')]

        logging.info(f"Retrieved {len(cast_data_list)} replies for user: {fid}...")
        return cast_data_list

    
    def collect_bootstrap_fids(self):
        try:
            fids = self.cyphers.collect_bootstrap_fids()
            fids_list = [i.get('fid') for i in fids]
            logging.info(f"Collected {len(fids_list)} fids for bootstrapping.")
            return fids_list
        except Exception as e:
            logging.error(f"Error retrieving bootstrap fids: {e}")
            return []

    # Modified methods to use multithreading
    def get_all_follows(self, fids):
        logging.info("Capturing follows relationships...")
        # Use parallel processing instead of sequential processing
        follows_list = self.mp.parallel_process(
            self.get_user_follows,
            fids,
            description="Processing follows for FIDs"
        )
        # Flatten the list of lists
        flattened_follows = [item for sublist in follows_list for item in sublist]
        return flattened_follows

    def get_all_replies(self, fids):
        logging.info("Capturing replies relationships...")
        # Use parallel processing
        replies_list = self.mp.parallel_process(
            self.get_user_casts,  # Note: fixed to use correct method here
            fids,
            description="Processing replies for FIDs"
        )
        # Flatten the list of lists
        flattened_replies = [item for sublist in replies_list for item in sublist]
        return flattened_replies

    def get_all_likes(self, fids):
        logging.info("Capturing likes relationships...")
        # Use parallel processing
        likes_list = self.mp.parallel_process(
            self.get_user_likes,
            fids,
            description="Processing likes for FIDs"
        )
        # Flatten the list of lists
        flattened_likes = [item for sublist in likes_list for item in sublist]
        return flattened_likes
    
    def get_all_recasts(self, fids):
        logging.info("Capturing recast relationships...")
        # Use parallel processing
        recasts_list = self.mp.parallel_process(
            self.get_user_recasts,
            fids,
            description="Processing recasts for FIDs"
        )
        # Flatten the list of lists
        flattened_recasts = [item for sublist in recasts_list for item in sublist]
        return flattened_recasts

    def run(self):
        logging.info("Starting Farcaster data collection...")
        
        # Get the last FID we've processed (or start from a default)

        
        # Fetch all new users by incrementing FIDs
        # logging.info("Fetching new users...")
        # users = self.fetch_users_until_end()
        # self.data['new_users'] = users

        # Fetching users to scrape...
        fids_to_process = self.collect_bootstrap_fids()
        
        # Process user network data
        logging.info(f"Processing network data for {len(fids_to_process)} users")
        
        # Get likes
        likes = self.get_all_likes(fids_to_process)
        self.data['likes'] = likes
        logging.info(f"Collected {len(likes)} likes")
        
        # Get recasts
        recasts = self.get_all_recasts(fids_to_process)
        self.data['recasts'] = recasts
        logging.info(f"Collected {len(recasts)} recasts")
        
        # Get follows
        follows = self.get_all_follows(fids_to_process)
        self.data['follows'] = follows
        logging.info(f"Collected {len(follows)} follows")
        
        # Get replies
        replies = self.get_all_replies(fids_to_process)
        self.data['replies'] = replies
        logging.info(f"Collected {len(replies)} replies")
        
        # Save all data
        self.save_data()
        logging.info("Data collection complete and saved")


if __name__ == "__main__":
    # Logging is now automatically configured by the Base class
    S = FcsScraper()
    S.run()