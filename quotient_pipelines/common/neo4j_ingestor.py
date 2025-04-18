# quotient_pipelines/common/neo4j_ingestor.py

import os
import logging
import pandas as pd
import boto3
import traceback
from typing import List, Dict, Any, Optional

from dagster import resource, InitResourceContext
from neo4j import GraphDatabase, BoltDriver

@resource
class Neo4jIngestor:
    """
    A utility class for ingesting data into Neo4j via S3 CSV files.
    This combines S3 utilities with Neo4j query execution.
    """
    
    def __init__(self, init_context: InitResourceContext):
        # Initialize AWS clients with region from env var or default to us-east-2
        self.region = "us-east-2"
        self.s3_client = boto3.client("s3", region_name=self.region)
        self.s3_resource = boto3.resource("s3", region_name=self.region)
        
        # Neo4j connection from env vars
        self.neo4j_uri = os.environ.get("NEO4J_URI")
        self.neo4j_user = "neo4j"
        self.neo4j_password = os.environ.get("NEO4J_PASSWORD")
        self.neo4j_database = None 
        
        # Store context for logging
        self.context = init_context
        
        # Log initialization
        self.context.log.info(f"Neo4jIngestor initialized with region: {self.region}")
    
    def ingest_dataframe(self, 
                        df: pd.DataFrame, 
                        bucket_name: str,
                        file_name: str, 
                        cypher_query: str, 
                        max_lines: int = 10000, 
                        max_size: int = 10000000):
        """
        Simplified workflow:
        1. Create bucket if it doesn't exist
        2. Save DataFrame to S3 as CSV (with chunking if needed)
        3. Execute Cypher query that loads this CSV 
        4. Directly log the results
        
        Returns:
            Total number of records processed
        """
        # Step 1: Create bucket if needed
        self._create_or_get_bucket(bucket_name)
        
        # Step 2: Save DataFrame to S3 as CSV
        csv_urls = self._save_df_as_csv(df, bucket_name, file_name, max_lines=max_lines, max_size=max_size)
        self.context.log.info(f"Saved {len(csv_urls)} CSV chunks to S3")
        
        # Step 3: Execute Cypher for each CSV URL and log results directly
        total_processed = 0
        
        for i, url in enumerate(csv_urls):
            try:
                # Format the Cypher query with the CSV URL
                formatted_query = cypher_query.format(csv_url=url)
                
                # Execute the query and log the direct response
                result = self.run_query(formatted_query)
                self.context.log.info(f"reallllllly important result {result}")
                
                # Log the actual result for better visibility
                self.context.log.info(f"Chunk {i+1}/{len(csv_urls)} result: {result}")
                
                # Track count if available
                if result and len(result) > 0 and 'count' in result[0]:
                    chunk_count = result[0]['count']
                    total_processed += chunk_count
                    self.context.log.info(f"Processed {chunk_count} records in chunk {i+1}")
                
            except Exception as e:
                self.context.log.error(f"Error processing chunk {i+1}/{len(csv_urls)}: {str(e)}")
                self.context.log.error(f"Query that failed: {formatted_query}")
                self.context.log.error(traceback.format_exc())
        
        self.context.log.info(f"Total records processed: {total_processed}")
        return total_processed    
    
    def run_query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a Cypher query against Neo4j and return results
        """
        driver = self._get_neo4j_driver()
        try:
            with driver.session(database=self.neo4j_database) as session:
                result = session.run(query, parameters or {})
                return [record.data() for record in result]
        finally:
            driver.close()
    
    def _get_neo4j_driver(self) -> BoltDriver:
        """Get a Neo4j driver using environment variables"""
        if not all([self.neo4j_uri, self.neo4j_user, self.neo4j_password]):
            self.context.log.error("Neo4j connection details missing")
            raise ValueError("Neo4j connection details not configured. Make sure NEO4J_URI, NEO4J_USER, and NEO4J_PASSWORD are set.")
        return GraphDatabase.driver(self.neo4j_uri, auth=(self.neo4j_user, self.neo4j_password))
    
    def _create_or_get_bucket(self, bucket_name: str) -> bool:
        """Create S3 bucket if it doesn't exist with proper configuration for Neo4j access"""
        response = self.s3_client.list_buckets()
        if bucket_name not in [el["Name"] for el in response["Buckets"]]:
            try:
                # Create bucket with specified region
                location = {"LocationConstraint": self.region}
                self.s3_client.create_bucket(
                    Bucket=bucket_name, 
                    CreateBucketConfiguration=location
                )
                self.context.log.info(f"Created bucket: {bucket_name}")
                
                # Configure bucket for public access
                self._configure_bucket(bucket_name)
                return True
            except Exception as e:
                self.context.log.error(f"Error creating bucket {bucket_name}: {str(e)}")
                raise
        else:
            self.context.log.info(f"Using existing bucket: {bucket_name}")
            # Ensure bucket has the right configuration
            self._configure_bucket(bucket_name)
            return False
    
    def _configure_bucket(self, bucket_name: str):
        """Configure S3 bucket for public access (needed for Neo4j LOAD CSV)"""
        try:
            self.s3_client.put_public_access_block(
                Bucket=bucket_name,
                PublicAccessBlockConfiguration={
                    'BlockPublicAcls': False,
                    'IgnorePublicAcls': False,
                    'BlockPublicPolicy': False,
                    'RestrictPublicBuckets': False
                }
            )
            
            self.s3_client.put_bucket_ownership_controls(
                Bucket=bucket_name,
                OwnershipControls={
                    'Rules': [
                        {
                            'ObjectOwnership': 'ObjectWriter'
                        },
                    ]
                }
            )
            self.context.log.info(f"Bucket {bucket_name} configured for public access")
        except Exception as e:
            self.context.log.error(f"Error configuring bucket {bucket_name}: {str(e)}")
            raise
    
    def _save_df_as_csv(self, 
                        df: pd.DataFrame,
                        bucket_name: str,
                        file_name: str, 
                        max_lines: int = 10000, 
                        max_size: int = 10000000) -> List[str]:
        """
        Save DataFrame to CSV files in S3, chunking if necessary.
        Returns list of public URLs to the CSV files.
        """
        chunks = [df]
        # Check if dataframe needs to be split
        if df.memory_usage(index=False).sum() > max_size or len(df) > max_lines:
            chunks = self._split_dataframe(df, chunk_size=max_lines)
        
        self.context.log.info(f"Saving DataFrame with {len(df)} rows as {len(chunks)} chunks")
        
        urls = []
        for chunk_id, chunk in enumerate(chunks):
            # Generate chunk filename
            chunk_name = f"{file_name}--{chunk_id}.csv"
            
            # Save to S3
            chunk.to_csv(f"s3://{bucket_name}/{chunk_name}", index=False, escapechar='\\')
            
            # Set public read permissions
            self.s3_resource.ObjectAcl(bucket_name, chunk_name).put(ACL='public-read')
            
            # Generate public URL
            location = self.s3_client.get_bucket_location(Bucket=bucket_name)["LocationConstraint"]
            location = location if location else 'us-east-1'  # Default region if None
            url = f"https://s3-{location}.amazonaws.com/{bucket_name}/{chunk_name}"
            urls.append(url)
            
            self.context.log.info(f"Saved chunk {chunk_id+1}/{len(chunks)} to {url}")
        
        return urls
    
    def _split_dataframe(self, df: pd.DataFrame, chunk_size: int = 10000) -> List[pd.DataFrame]:
        """Split a DataFrame into chunks of specified size"""
        chunks = []
        num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size else 0)
        for i in range(num_chunks):
            chunks.append(df[i * chunk_size:(i + 1) * chunk_size])
        return chunks