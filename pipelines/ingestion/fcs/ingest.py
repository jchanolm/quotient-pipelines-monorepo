from ..helpers import Ingestor
from .cyphers import FcsIngestCyphers
import datetime
import pandas as pd
from typing import Dict, List, Any
import logging


class FcsIngestor(Ingestor):
    def __init__(self):
        self.cyphers = FcsIngestCyphers()
        super().__init__("fcs")

    def explore_data(self):
        print("Loading data....")
        data = self.scraper_data
        likes_df = pd.DataFrame(data['likes'])
        print(likes_df.head(25))


    def run(self):
        self.explore_data()


if __name__ == "__main__":
    ingestor = FcsIngestor()
    ingestor.run()