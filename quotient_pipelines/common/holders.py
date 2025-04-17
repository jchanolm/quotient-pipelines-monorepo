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

# Op: fetch token holders from Ankr and ingest them into Neo4j
@op(required_resource_keys={'neo4j'}, description="Fetch holders and ingest to Neo4j")
def fetch_and_ingest_holders(context, contract_address: str):
    url = os.getenv("ANKR_RPC_URL")
    if not url:
        context.log.error("ANKR_RPC_URL is not set in environment variables")
        return 0
        
    page_token = None
    total_holders = 0

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
                
            # Create records for Neo4j
            try:
                records = df.to_dict('records')
                params_list = []
                for r in records:
                    try:
                        params_list.append({
                            "address": r['holderAddress'].lower(),
                            "balance": float(r['balance']),
                            "balanceRaw": r['balanceRawInteger']
                        })
                    except (KeyError, ValueError) as e:
                        context.log.warning(f"Error processing record {r}: {str(e)}")
                
                context.log.info(f"Prepared {len(params_list)} records for Neo4j ingestion from {page_count} holders")
                context.log.info(f"Neo4j query params sample (first 2): {params_list[:2]}")
            except Exception as e:
                context.log.error(f"Failed to process dataframe records: {str(e)}")
                context.log.error(traceback.format_exc())
                break

            # Ingest to Neo4j
            try:
                # Batch create relationships
                cypher = """
                UNWIND $holders AS h
                MERGE (w:Wallet {address: h.address})
                WITH w
                MATCH (t:Token {address: $token_address})
                MERGE (w)-[r:HOLDS]->(t)
                SET r.balance = tofloat(h.balance), r.balanceRaw = tofloat(h.balanceRaw), r.lastUpdated = datetime()
                RETURN COUNT(*)
                """

                context.log.info(f"Executing Neo4j query for {len(params_list)} holders")
                context.resources.neo4j.run_query(cypher, holders=params_list, token_address=contract_address.lower())
                context.log.info(f"Successfully ingested {len(params_list)} holders into Neo4j for {contract_address}")
                
                # Verify data was inserted
                try:
                    verify_query = f"""
                    MATCH (t:Token {{address: '{contract_address.lower()}'}})<-[r:HOLDS]-(w:Wallet) 
                    RETURN COUNT(w) as count
                    """
                    context.log.info(f"Running verification query: {verify_query}")
                    verify_result = context.resources.neo4j.run_query(verify_query)
                    context.log.info(f"Verification query result: {verify_result}")
                    holder_count = verify_result[0]['count'] if verify_result else 0
                    context.log.info(f"Verified {holder_count} total holders for {contract_address} in Neo4j")
                except Exception as e:
                    context.log.error(f"Verification query failed: {str(e)}")
                    context.log.error(traceback.format_exc())
            except Exception as e:
                context.log.error(f"Failed to ingest holders into Neo4j: {str(e)}")
                context.log.error(traceback.format_exc())
                break
                
            total_holders += page_count
            
            # Check if there are more pages
            page_token = result.get('nextPageToken')
            if not page_token:
                context.log.info(f"No more pages for {contract_address}")
                break

            time.sleep(1)
        except Exception as e:
            context.log.error(f"Unexpected error processing holders for {contract_address}: {str(e)}")
            context.log.error(traceback.format_exc())
            break
    
    context.log.info(f"Completed processing {total_holders} total holders for {contract_address}")
    return total_holders