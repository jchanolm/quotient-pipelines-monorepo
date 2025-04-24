from ...helpers import Cypher
from ...helpers import Utils
import datetime  
from ...helpers import count_query_logging

class FcsScraperCyphers(Cypher):

    def __init__(self, database=None):
        super().__init__(database)
        self.asOf = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M")

    @count_query_logging
    def collect_bootstrap_fids(self):
        query = """
        MATCH (wc:Warpcast:Account)
        WHERE wc.fcCredScore >= 20
        AND NOT (wc)-[:LIKED]->()
        RETURN DISTINCT wc.fid as fid
        ORDER BY fid ASC
        """
        results = self.query(query)
        return results

    @count_query_logging
    def get_last_fid(self):
        query = """
        MATCH (wc:Warpcast:Account)
        RETURN MAX(wc.fid) as fid
        """
        results = self.query(query)[0]
        return results