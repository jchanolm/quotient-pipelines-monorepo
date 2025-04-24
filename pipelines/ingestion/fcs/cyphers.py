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
            MERGE (source:Warpcast:Account:WarpcastAccount {{fid: tointeger(row['source'])}})
            ON CREATE SET
                source.needsEnrichment = True 
            WITH source, row 
            MERGE (target:Warpcast:Account:WarpcastAccount {{fid: tointeger(row['target'])}})
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

    @count_query_logging
    def create_follows_relationships(self, urls):
        count = 0 
        url_counter = 0
        for url in urls:
            url_counter += 1
            logging.info(f"Creating for {url_counter} out of {len(urls)}...")
            query = f""" 
            LOAD CSV WITH HEADERS FROM '{url}' as row
            MATCH (source:Warpcast:Account {{fid: tointeger(row['source'])}})
            WITH source , row
            MERGE (target:Warpcast:Account:WarpcastAccount {{fid: tointeger(row['target'])}})
            ON CREATE SET
                target.needsEnrichment = True 
            WITH source, target, row 
            MERGE (source)-[r:FOLLOWED]->(target)
            SET r.timestamp = tointeger(row['timestamp'])
            SET r.bucketIndex = tointeger(row['bucketIndex'])
            SET r.bucketEndTimestamp = tointeger(row['bucket_end_timestamp'])
            SET r.bucketStartTimestamp = tointeger(row['bucket_start_timestamp'])
            SET r.count = 25
            SET r.bucketEndDtReadable = tointeger(row['bucket_start_dt_readable'])
            SET r.bucketStartDtReadable = tointeger(row['bucket_start_dt_readable'])
            RETURN COUNT(*)
            """
            print(query)
            count += self.query(query)[0].value()
        return count 
    
    @count_query_logging 
    def create_replies_relationships(self, urls):
        count =0 
        for url in urls:
            print(len(urls))
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS row
            MATCH (source:WarpcastAccount {{fid: tointeger(row['source'])}})
            MATCH (target:WarpcastAccount {{fid: tointeger(row['target'])}})
            MERGE (source)-[r:REPLIED]->(target)
            SET r.timestamp = tointeger(row['timestamp'])
            SET r.bucketIndex = tointeger(row['bucketIndex'])
            SET r.bucketEndTimestamp = tointeger(row['bucket_end_timestamp'])
            SET r.bucketStartTimestamp = tointeger(row['bucket_start_timestamp'])
            SET r.count = tointeger(row['count'])
            SET r.bucketEndDtReadable = tointeger(row['bucket_start_dt_readable'])
            SET r.bucketStartDtReadable = tointeger(row['bucket_start_dt_readable'])
            RETURN COUNT(*)
            """
            print(query)
            count += self.query(query)[0].value()
        return count 