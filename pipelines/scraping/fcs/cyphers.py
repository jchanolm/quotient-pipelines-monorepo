from ...helpers import Cypher
from ...helpers import Utils
import datetime  
from ...helpers import count_query_logging

class FcsScraperCyphers(Cypher):

    def __init__(self, database=None):
        super().__init__(database)
        self.asOf = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M")

    def collect_bootstrap_fids(self):

        query = """
        MATCH (wc:Warpcast:Account)
        WHERE wc.fcCredScore > 19
        RETURN DISTINCT wc.fid as fid
        """
        results = self.query(query)

        return results


