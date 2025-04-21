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

        # Setup logging configuration FIRST, before any other initialization
        self.setup_logging()
        
        # Log that we're initializing
        logging.info(f"Initializing {self.__class__.__name__}")

        if bucket_name:
            S3Utils.__init__(self, bucket_name, metadata_filename, load_data)
        Multiprocessing.__init__(self)
        Utils.__init__(self)
    
    @staticmethod
    def setup_logging(level=logging.INFO):
        """
        Configure logging with a standard format and level.
        Will force configuration even if already configured.
        """
        # Force reconfiguration of the root logger
        root = logging.getLogger()
        if root.handlers:
            for handler in root.handlers:
                root.removeHandler(handler)
        
        # Set up basic configuration
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout)
            ],
            force=True  # This ensures we override any existing configuration
        )
        
        # Set custom levels for noisy libraries
        logging.getLogger("boto3").setLevel(logging.WARNING)
        logging.getLogger("botocore").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("s3transfer").setLevel(logging.WARNING)
        
        # Log that logging is set up
        logging.info("Logging configured")