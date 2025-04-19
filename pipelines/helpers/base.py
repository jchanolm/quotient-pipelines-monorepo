from datetime import datetime
import os
import sys
import logging
from . import S3Utils
from . import Utils
from . import Multiprocessing

from dotenv import load_dotenv

load_dotenv()

class Base(S3Utils, Multiprocessing, Utils):
    def __init__(self, bucket_name, metadata_filename, load_data, chain) -> None:
        self.runtime = datetime.now()
        self.asOf = f"{self.runtime.year}-{self.runtime.month}-{self.runtime.day}"
        self.isAirflow = os.environ.get("IS_AIRFLOW", False)

        # Setup logging configuration
        self.setup_logging()

        if bucket_name:
            S3Utils.__init__(self, bucket_name, metadata_filename, load_data)
        Multiprocessing.__init__(self)
        Utils.__init__(self)
    
    @staticmethod
    def setup_logging(level=logging.INFO):
        """
        Configure logging with a standard format and level.
        Will only configure if it hasn't been configured already.
        """
        # Don't setup logging again if root logger already has handlers
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=level,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.StreamHandler(sys.stdout)
                ]
            )
            
            # Quiet some commonly noisy loggers
            logging.getLogger("boto3").setLevel(logging.WARNING)
            logging.getLogger("botocore").setLevel(logging.WARNING)
            logging.getLogger("urllib3").setLevel(logging.WARNING)
            logging.getLogger("s3transfer").setLevel(logging.WARNING)
