from ...helpers import Cypher
from ...helpers import count_query_logging
import logging 

class FcsIngestCyphers(Cypher):
    def __init__(self):
        super().__init__()

    @count_query_logging
    def create_likes(self, urls):
        count = 0
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS row 
            MERGE (source:Warpcast:Account {{fid: tointeger(row['source'])}})
            ON CREATE SET
                source.needsEnrichment = True 
            WITH source, row 
            MERGE (target:Warpcast:Account {{fid: tointeger(row['target'])}})
            ON CREATE SET
                target.needsEnrichment = True
            WITH source, target, row 
            MERGE (source)-[r:LIKED {{bucketIndex: tointeger(row['bucket_index'])}}]->(target)
            SET r.count = tointeger(row['count'])
            SET r.bucketStartTimestamp = tointeger(row['bucket_start_timestamp'])
            SET r.bucketEndTimestamp = tointeger(row['bucket_end_timestamp']) 
            SET r.bucketStartDtReadable = row['bucket_start_dt_readable']
            SET r.bucketEndDtReadable = row['bucket_end_dt_readable']
            SET r.bucketLabel = row['bucket_label']
            RETURN COUNT(r)           
            """
            count += self.query(query)[0].value()
        return count 


    
    @count_query_logging 
    def create_recasts(self, urls):
        count_urls = len(urls)
        print(f"Commencing with {count_urls} urls...")
        urls_counter = 0
        count = 0 
        for url in urls:
            urls_counter += 1
            print(f"Creating url {str(urls_counter)} out of {count_urls}")
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS row 
            MERGE (source:Warpcast:Account {{fid: tointeger(row['source'])}})
            ON CREATE SET
                source.needsEnrichment = True 
            WITH source, row 
            MERGE (target:Warpcast:Account {{fid: tointeger(row['target'])}})
            ON CREATE SET
                target.needsEnrichment = True
            WITH source, target, row 
            MERGE (source)-[r:RECASTED {{bucketIndex: tointeger(row['bucket_index'])}}]->(target)
            SET r.count = tointeger(row['count'])
            SET r.bucketStartTimestamp = tointeger(row['bucket_start_timestamp'])
            SET r.bucketEndTimestamp = tointeger(row['bucket_end_timestamp']) 
            SET r.bucketStartDtReadable = row['bucket_start_dt_readable']
            SET r.bucketEndDtReadable = row['bucket_end_dt_readable']
            SET r.bucketLabel = row['bucket_label']
            RETURN COUNT(r)           
            """
            count += self.query(query)[0].value()
        return count 
