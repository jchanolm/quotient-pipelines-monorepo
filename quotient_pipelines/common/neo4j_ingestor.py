# quotient_pipelines/common/neo4j_ingestor.py

import os
import logging
import pandas as pd
import boto3
from typing import List, Dict, Any, Optional

from dagster import ConfigurableResource
from neo4j import GraphDatabase, BoltDriver

class Neo4jIngestor(ConfigurableResource):
    """
    A utility class for ingesting data into Neo4j via S3 CSV files.
    This combines S3 utilities with Neo4j query execution.
    
    Configuration:
    - bucket_name: S3 bucket to use for storing CSV files
    - region: AWS region for the bucket (default: us-east-2)
    """
    
    def setup_resource(self, _):
        # Initialize AWS clients with region from env var or default to us-east-2
        self.region = "us-east-2"
        self.s3_client = boto3.client("s3", region_name=self.region)
        self.s3_resource = boto3.resource("s3", region_name=self.region)
        
        # Neo4j connection from env vars
        self.neo4j_uri = os.environ.get("NEO4J_URI")
        self.neo4j_user = "neo4j"
        self.neo4j_password = os.environ.get("NEO4J_PASSWORD")
        self.neo4j_database = None 
        
        # Log initialization
        logging.info(f"Neo4jIngestor initialized with region: {self.region}")
    
    def ingest_dataframe(self, 
                         df: pd.DataFrame, 
                         bucket_name: str,
                         file_name: str, 
                         cypher_query: str, 
                         max_lines: int = 10000, 
                         max_size: int = 10000000) -> Dict[str, Any]:
        """
        Main workflow function:
        1. Create bucket if it doesn't exist
        2. Save DataFrame to S3 as CSV (with chunking if needed)
        3. Execute Cypher query that loads this CSV 
        4. Return statistics about the operation
        
        Args:
            df: The pandas DataFrame to ingest
            bucket_name: S3 bucket to use (will be created if needed)
            file_name: Base name for the CSV file(s)
            cypher_query: Cypher query template that will use LOAD CSV
                         (Should include {csv_url} placeholder)
            max_lines: Maximum rows per CSV chunk
            max_size: Maximum bytes per CSV chunk
            
        Returns:
            Dictionary with stats about the operation
        """
        # Step 1: Create bucket if needed
        self._create_or_get_bucket(bucket_name)
        
        # Step 2: Save DataFrame to S3 as CSV
        csv_urls = self._save_df_as_csv(df, bucket_name, file_name, max_lines=max_lines, max_size=max_size)
        logging.info(f"Saved {len(csv_urls)} CSV chunks to S3")
        
        # Step 3: Execute Cypher for each CSV URL
        results = []
        successful_chunks = 0
        total_records = 0
        
        for i, url in enumerate(csv_urls):
            try:
                # Format the Cypher query with the CSV URL
                formatted_query = cypher_query.format(csv_url=url)
                
                # Execute the query
                result = self.run_query(formatted_query)
                results.append(result)
                
                # Track statistics
                successful_chunks += 1
                if result and len(result) > 0 and 'count' in result[0]:
                    total_records += result[0]['count']
                
                logging.info(f"Successfully processed chunk {i+1}/{len(csv_urls)}")
            except Exception as e:
                logging.error(f"Error processing chunk {i+1}/{len(csv_urls)}: {str(e)}")
        
        # Return statistics
        return {
            "file_name": file_name,
            "total_chunks": len(csv_urls),
            "successful_chunks": successful_chunks,
            "total_records": total_records,
            "csv_urls": csv_urls,
            "results": results
        }
    
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
            logging.error("Neo4j connection details missing")
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
                logging.info(f"Created bucket: {bucket_name}")
                
                # Configure bucket for public access
                self._configure_bucket(bucket_name)
                return True
            except Exception as e:
                logging.error(f"Error creating bucket {bucket_name}: {str(e)}")
                raise
        else:
            logging.info(f"Using existing bucket: {bucket_name}")
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
            logging.info(f"Bucket {bucket_name} configured for public access")
        except Exception as e:
            logging.error(f"Error configuring bucket {bucket_name}: {str(e)}")
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
        
        logging.info(f"Saving DataFrame with {len(df)} rows as {len(chunks)} chunks")
        
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
            
            logging.info(f"Saved chunk {chunk_id+1}/{len(chunks)} to {url}")
        
        return urls
    
    def _split_dataframe(self, df: pd.DataFrame, chunk_size: int = 10000) -> List[pd.DataFrame]:
        """Split a DataFrame into chunks of specified size"""
        chunks = []
        num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size else 0)
        for i in range(num_chunks):
            chunks.append(df[i * chunk_size:(i + 1) * chunk_size])
        return chunks