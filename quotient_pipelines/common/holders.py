# quotient_pipelines/common/holders.py
import os
import time
import pandas as pd
from dagster import op
import requests
from dotenv import load_dotenv
import traceback

# Load environment variables
dotenv_loaded = load_dotenv()

# Op: fetch token holders from Ankr and ingest them into Neo4j via S3
@op(required_resource_keys={'neo4j_ingestor'}, description="Fetch holders and ingest to Neo4j via S3")
def fetch_and_ingest_holders(context, contract_address: str):
    url = os.getenv("ANKR_RPC_URL")
    if not url:
        context.log.error("ANKR_RPC_URL is not set in environment variables")
        return 0
        
    page_token = None
    total_holders = 0
    all_holders_df = None  # This will hold all holders across pages for the current token

    # Step 1: Fetch all holders data from Ankr API (paginated)
    while True:
        try:
            # Prepare request
            payload = {
                "id": 1,
                "jsonrpc": "2.0",
                "method": "ankr_getTokenHolders",
                "params": {"blockchain": "base", "contractAddress": contract_address, "pageSize": 10000}
            }
            if page_token:
                payload["params"]["pageToken"] = page_token

            # Fetch holders from Ankr
            context.log.info(f"Fetching holders for {contract_address} with page token: {page_token or 'first page'}")
            resp = requests.post(url, json=payload)
            resp.raise_for_status()
            result = resp.json().get('result', {})
            holders = result.get('holders', [])
            
            if not holders:
                context.log.info(f"No holders found for {contract_address} with page token: {page_token or 'first page'}")
                break

            # Process holder data
            context.log.info(f"Processing {len(holders)} holders for {contract_address}")
            df = pd.DataFrame(holders)
            page_count = len(df)
            
            # Verify dataframe has expected columns
            expected_columns = ['holderAddress', 'balance', 'balanceRawInteger']
            missing_columns = [col for col in expected_columns if col not in df.columns]
            if missing_columns:
                context.log.error(f"Missing columns in holder data: {missing_columns}")
                context.log.info(f"Available columns: {df.columns.tolist()}")
                context.log.info(f"Sample data: {df.head(1).to_dict('records')}")
                break
                
            # Format DataFrame with column names suitable for Neo4j
            df['address'] = df['holderAddress'].str.lower()
            df['balance'] = df['balance'].astype(float) 
            df['balanceRaw'] = df['balanceRawInteger']
            df = df[['address', 'balance', 'balanceRaw']]  # Keep only columns we need
            
            # Append to the full holders dataframe for this token
            if all_holders_df is None:
                all_holders_df = df
            else:
                all_holders_df = pd.concat([all_holders_df, df], ignore_index=True)
            
            total_holders += page_count
            
            # Check if there are more pages
            page_token = result.get('nextPageToken')
            if not page_token:
                context.log.info(f"No more pages for {contract_address}")
                break

            time.sleep(1)  # Avoid rate limiting
        except Exception as e:
            context.log.error(f"Unexpected error processing holders for {contract_address}: {str(e)}")
            context.log.error(traceback.format_exc())
            break
    
    # Step 2: Save data to S3 and ingest into Neo4j
    if all_holders_df is not None and not all_holders_df.empty:
        try:
            context.log.info(f"Preparing to ingest {len(all_holders_df)} holders for {contract_address}")
            
            # Create bucket name with environment prefix
            bucket_name = f"quotient-pclank-token-holders"
            file_name = f"holders-{contract_address.lower()}-{int(time.time())}"
            
            # Create LOAD CSV Cypher query with the token address hard-coded
            # Simplified query that returns count directly
            cypher_query = f"""
            LOAD CSV WITH HEADERS FROM '{{csv_url}}' AS row
            MERGE (w:Wallet {{address: row.address}})
            WITH w, row
            MATCH (t:Token {{address: '{contract_address.lower()}'}})
            MERGE (w)-[r:HOLDS]->(t)
            SET r.balance = tofloat(row.balance), 
                r.balanceRaw = tofloat(row.balanceRaw), 
                r.lastUpdated = datetime()
            RETURN count(r) AS count
            """
            context.log.info(cypher_query)
            
            # Use ingestor to save to S3 and run the Cypher query
            result = context.resources.neo4j_ingestor.ingest_dataframe(
                df=all_holders_df,
                bucket_name=bucket_name,
                file_name=file_name,
                cypher_query=cypher_query
            )
            
            context.log.info(result)
            # Log details about the ingestion
            context.log.info(f"S3 CSV ingestion completed: {result['successful_chunks']}/{result['total_chunks']} chunks processed")
            
            # Get total_records from results if available
            total_ingested = 0
            
            for i, chunk_result in enumerate(result.get('results', [])):
                if chunk_result and len(chunk_result) > 0:
                    chunk_count = chunk_result[0].value()
                    total_ingested += chunk_count
                    context.log.info(f"Chunk {i+1}: Ingested {chunk_count} records")
                else:
                    context.log.warning(f"Chunk {i+1}: Could not determine number of records ingested")
                    context.log.info(f"Chunk result structure: {chunk_result}")
            
            context.log.info(f"Total records ingested across all chunks: {total_ingested}")
            
            # Verify data was inserted with a direct query
            try:
                verify_query = f"""
                MATCH (t:Token {{address: '{contract_address.lower()}'}})<-[r:HOLDS]-(w:Wallet) 
                RETURN COUNT(w) as count
                """
                verify_result = context.resources.neo4j_ingestor.run_query(verify_query)
                holder_count = verify_result[0]['count'] if verify_result else 0
                context.log.info(f"Verified {holder_count} total holders for {contract_address} in Neo4j")
            except Exception as e:
                context.log.error(f"Verification query failed: {str(e)}")
                context.log.error(traceback.format_exc())
                
        except Exception as e:
            context.log.error(f"Failed to ingest holders into Neo4j: {str(e)}")
            context.log.error(traceback.format_exc())
    else:
        context.log.warning(f"No holder data to ingest for {contract_address}")
    
    context.log.info(f"Completed processing {total_holders} total holders for {contract_address}")
    return total_holders