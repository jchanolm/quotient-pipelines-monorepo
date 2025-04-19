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

from ..helpers.scraper import Scraper
from .cyphers import FcsScraperCyphers

load_dotenv()

class Multiprocessing:
    def __init__(self) -> None:
        self.max_thread = max(8, multiprocessing.cpu_count() * 2)
        if os.environ.get("DEBUG", False):
            self.max_thread = multiprocessing.cpu_count() - 1
        os.environ["NUMEXPR_MAX_THREADS"] = str(self.max_thread)

    @contextlib.contextmanager
    def tqdm_joblib(self, tqdm_object):
        """Context manager to patch joblib to report into tqdm progress bar given as argument"""

        class TqdmBatchCompletionCallback(joblib.parallel.BatchCompletionCallBack):
            def __call__(self, *args, **kwargs):
                tqdm_object.update(n=self.batch_size)
                return super().__call__(*args, **kwargs)

        old_batch_callback = joblib.parallel.BatchCompletionCallBack
        joblib.parallel.BatchCompletionCallBack = TqdmBatchCompletionCallback
        try:
            yield tqdm_object
        finally:
            joblib.parallel.BatchCompletionCallBack = old_batch_callback
            tqdm_object.close()

    def parallel_process(self, 
                         function, 
                         array: list, 
                         description: str = "Multithreaded processing running... Give me a description!") -> list:
        """
        Wrapper to execute a function as a parallel process using jobLib. 
        """
        with self.tqdm_joblib(tqdm(desc=description, total=len(array))):
            data = joblib.Parallel(n_jobs=self.max_thread, backend="threading")(joblib.delayed(function)(element) for element in array)
        return data


class FcsScraper(Scraper):
    def __init__(self, bucket_name="fcs", load_data=False, weeks=12):
        super().__init__(bucket_name=bucket_name, load_data=load_data)
        self.cyphers = FcsScraperCyphers()
        self.FARCASTER_EPOCH = datetime(2021, 1, 1, tzinfo=timezone.utc)
        self.NEYNAR_API_KEY = os.getenv('NEYNAR_API_KEY')
        self.cutoff_timestamp = None
        if self.cutoff_timestamp is not None:
            current_time = datetime.now(timezone.utc)
            self.cutoff_timestamp = (current_time - timedelta(days=7 * weeks)).timestamp()
        # Initialize multiprocessing
        self.mp = Multiprocessing()

    # Your existing methods remain the same...
    def convert_timestamp(self, timestamp):
        """Convert Farcaster timestamp to UTC datetime."""
        dt = self.FARCASTER_EPOCH + timedelta(seconds=int(timestamp))
        return dt.timestamp()  # Return Unix timestamp as float

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
        req_params['pageSize'] = 1000

        all_messages = []
        max_retries = 3
        initial_retry_delay = 1.0

        # === retry/backoff for this request ===
        retry_delay = initial_retry_delay
        for attempt in range(max_retries):
            try:
                time.sleep(0.1)
                logging.debug(f"GET {url} params={req_params!r}")
                response = r.get(url, headers=headers, params=req_params)

                if response.status_code != 200:
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
        for msg in msgs:
            ts = msg.get('data', {}).get('timestamp')
            if ts is not None:
                msg['data']['timestamp'] = int(self.convert_timestamp(ts))
        all_messages.extend(msgs)
        logging.info(f"Retrieved {len(all_messages)} messages total…")

        return all_messages
        
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
            'edge_type': 'FOLLOWS'
        } for item in messages 
          if "data" in item 
          and "linkBody" in item["data"] 
          and item['data']['linkBody'].get('targetFid') 
          and item['data'].get('timestamp')]

    def get_user_likes(self, fid):
        endpoint = "reactionsByFid"
        params = {
            'fid': fid,
            'reaction_type': 'Like'
        }
        messages = self.query_neynar_hub(endpoint, params)

        return [{
            'source': fid,
            'target': item['data']['reactionBody']['targetCastId'].get('fid'),
            'target_hash': item['data']['reactionBody']['targetCastId'].get('hash'),
            'timestamp': item['data'].get('timestamp'),
            'edge_type': 'LIKED'
        } for item in messages 
          if "data" in item 
          and "reactionBody" in item["data"] 
          and item['data']['reactionBody'].get('targetCastId') 
          and item['data'].get('timestamp')]

    def get_user_recasts(self, fid):
        endpoint = "reactionsByFid"
        params = {
            'fid': fid,
            'reaction_type': 'Recast'
        }
        messages = self.query_neynar_hub(endpoint, params)

        return [{
            'source': fid,
            'target': item['data']['reactionBody']['targetCastId'].get('fid'),
            'target_hash': item['data']['reactionBody']['targetCastId'].get('hash'),
            'timestamp': item['data'].get('timestamp'),
            'edge_type': 'RECASTED'
        } for item in messages 
        if "data" in item 
        and "reactionBody" in item["data"] 
        and item['data']['reactionBody'].get('targetCastId') 
        and item['data'].get('timestamp')]

    def get_user_casts(self, fid):
        logging.info(f"Collecting casts for user {fid}.....")
        endpoint = "castsByFid"
        params = {'fid': fid}
        messages = self.query_neynar_hub(endpoint=endpoint, params=params)

        cast_data_list = [{
            'source': fid,
            'target': message['data']['castAddBody']['parentCastId']['fid'],
            'timestamp': message['data']['timestamp'],
            'edge_type': 'REPLIED'
        } for message in messages 
          if 'data' in message 
          and 'castAddBody' in message['data'] 
          and message['data']['castAddBody'].get('parentCastId')]

        logging.info(f"Retrieved {len(cast_data_list)} replies for user: {fid}...")
        return cast_data_list

    def get_user_data(self, fid):
        return {
            'core_node_metadata': self.get_user_metadata(fid),
            'likes': self.get_user_likes(fid),
            'recasts': self.get_user_recasts(fid),
            'casts': self.get_user_casts(fid),
            'following': self.get_user_follows(fid)
        }
    
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
        return follows_list

    def get_all_replies(self, fids):
        logging.info("Capturing replies relationships...")
        # Use parallel processing
        replies_list = self.mp.parallel_process(
            self.get_user_casts,  # Note: fixed to use correct method here
            fids,
            description="Processing replies for FIDs"
        )
        return replies_list

    def get_all_likes(self, fids):
        logging.info("Capturing likes relationships...")
        # Use parallel processing
        likes_list = self.mp.parallel_process(
            self.get_user_likes,
            fids,
            description="Processing likes for FIDs"
        )
        return likes_list
    
    def get_all_recasts(self, fids):
        logging.info("Capturing recast relationships...")
        # Use parallel processing
        recasts_list = self.mp.parallel_process(
            self.get_user_recasts,
            fids,
            description="Processing recasts for FIDs"
        )
        return recasts_list

    def run(self):
        logging.info("Collecting bootstrap FIDs...")
        bootstrap_fids = self.collect_bootstrap_fids()
        
                ## get likes 
        likes = self.get_all_likes(bootstrap_fids)
        self.data['likes'] = likes


        # ## get followers
        # follows = self.get_all_follows(bootstrap_fids)
        # self.data['follows'] = follows

        # ## get replies
        # replies = self.get_all_replies(bootstrap_fids)
        # self.data['replies'] = replies

        # ## get likes 
        # likes = self.get_all_likes(bootstrap_fids)
        # self.data['likes'] = likes

        # ## get recasts
        # recasts = self.get_all_recasts(bootstrap_fids)
        # self.data['recasts'] = recasts

        self.save_data()


if __name__ == "__main__":
    # Logging is now automatically configured by the Base class
    S = FcsScraper()
    S.run()