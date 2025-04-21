import logging 
from datetime import datetime 
import pandas as pd 
    
def readable_timestamp(self, timestamp):
        try:
            # Convert timestamp to datetime object
            dt = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
            
            # Format as a readable string
            return dt
        except (ValueError, TypeError) as e:
            logging.error(f"Error converting timestamp {timestamp}: {str(e)}")
            return "Invalid timestamp"

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
