from ..helpers import Ingestor
import time
from .cyphers import FcsIngestCyphers
import datetime
import pandas as pd
from typing import Dict, List, Any
import logging


class FcsIngestor(Ingestor):
    def __init__(self):
        self.cyphers = FcsIngestCyphers()
        super().__init__("fcs")



    def readable_timestamp(self, timestamp):
        try:
            # Convert timestamp to datetime object
            dt = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
            
            # Format as a readable string
            return dt
        except (ValueError, TypeError) as e:
            logging.error(f"Error converting timestamp {timestamp}: {str(e)}")
            return "Invalid timestamp"

    def load_likes_data(self):
        logging.info("Loading likes data....")
        likes_data = self.scraper_data['likes']

        logging.info("Converting likes objects into DF....")
        likes_df = pd.DataFrame(likes_data)
        return likes_df
    
    def load_recasts_data(self):
        logging.info("Loading recasts....")
        recasts_data = self.scraper_data['recasts']

        logging.info("Converting recasts into dataframe...")
        recasts_df = pd.DataFrame(recasts_data)
        return recasts_df

    
    def bucket_data(self, df, bucket_size_days=7):
            logging.info("Bucketing data...")
            """
            Process likes data into biweekly buckets (7-day periods) using raw timestamps
            
            Args:
                likes_df: DataFrame with timestamp column
                bucket_size_days: Number of days per bucket (default: 7 for weekly)
            
            Returns:
                DataFrame with bucketed data and counts
            """
            # Ensure we have a timestamp column
            print(df.columns)
            if 'timestamp' not in df.columns:
                raise ValueError("DataFrame must contain 'timestamp' column")
            
            # Calculate bucket size in seconds
            bucket_size_seconds = bucket_size_days * 24 * 60 * 60
            
            # Find the global min timestamp to establish a reference point
            min_timestamp = df['timestamp'].min()
            
            # Create bucket index for each timestamp
            df['bucket_index'] = ((df['timestamp'] - min_timestamp) / bucket_size_seconds).astype(int)
            
            # Calculate start and end timestamps for each bucket
            df['bucket_start_timestamp'] = min_timestamp + (df['bucket_index'] * bucket_size_seconds)
            df['bucket_end_timestamp'] = df['bucket_start_timestamp'] + bucket_size_seconds - 1
            
            # Group by source, target, and bucket to count interactions
            grouped = df.groupby(['source', 'target', 'bucket_index', 
                                    'bucket_start_timestamp', 'bucket_end_timestamp']).size().reset_index(name='count')
            
            # Add human readable dates to the grouped data
            grouped['bucket_start_dt_readable'] = grouped['bucket_start_timestamp'].apply(self.readable_timestamp)
            grouped['bucket_end_dt_readable'] = grouped['bucket_end_timestamp'].apply(self.readable_timestamp)
            
            # Create a formatted label for display
            grouped['bucket_label'] = grouped.apply(
                lambda row: f"{row['bucket_start_dt_readable']} to {row['bucket_end_dt_readable']}", axis=1
            )
                    
            return grouped
    
    def ingest_likes_data(self):
        likes_df = self.load_likes_data()
        bucketed_likes_df = self.bucket_data(likes_df)
        likes_urls = self.save_df_as_csv(bucketed_likes_df, f"likes_data_from_{self.asOf}")
        self.cyphers.create_likes(likes_urls)

    def ingest_recasts_data(self):
        recasts_df = self.load_recasts_data()
        bucketed_recasts_df = self.bucket_data(recasts_df)
        recasts_urls = self.save_df_as_csv(bucketed_recasts_df, f"recasts_data_from_{self.asOf}")
        self.cyphers.create_recasts(recasts_urls)

    def load_follows_data(self):
        follows_data = self.scraper_data['follows']
        follows_df = pd.DataFrame(follows_data)
        bucketed_follows = self.bucket_data(follows_df)
        return bucketed_follows 
    
    def ingest_follows_data(self):
        follows_df = self.load_follows_data()
        follows_urls = self.save_df_as_csv(follows_df, f'follows_data_{self.asOf}')
        self.cyphers.create_follows_relationships(follows_urls)

    def ingest_follows_data_temp(self):
        follows_df = pd.DataFrame(self.scraper_data['follows'])
        follows_df['source'] = follows_df['source'].apply(lambda x: x[0])
        follows_urls = self.save_df_as_csv(follows_df, f"whoa_follows_data_{self.asOf}")
        self.cyphers.create_follows_relationships_temp(follows_urls)

    def load_replies_data(self):
        replies_data = self.scraper_data['replies']
        replies_df = pd.DataFrame(replies_data)
        bucketed_replies = self.bucket_data(replies_df)
        return bucketed_replies
    
    def ingest_replies_data(self):
        replies_data = self.load_replies_data()
        replies_urls = self.save_df_as_csv(replies_data, f"replies_data_{self.asOf}")
        self.cyphers.create_replies_relationships(replies_urls)

    def run(self):
        # self.ingest_likes_data()
        self.ingest_follows_data_temp()
        # self.ingest_recasts_data()
        # self.ingest_replies_data()

if __name__ == "__main__":
    ingestor = FcsIngestor()
    ingestor.run()