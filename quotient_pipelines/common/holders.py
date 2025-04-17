import os
import time
import pandas as pd
from dagster import op, graph, DynamicOut, DynamicOutput, Definitions
from dagster import ResourceDefinition
from web3 import Web3
import requests
from dotenv import load_dotenv
from quotient_pipelines.common.resources import neo4j_resource

# Load environment variables
dotenv_loaded = load_dotenv()

# Op: fetch token holders from Ankr
@op(out=DynamicOut(), description="Fetch paginated token holders for Base via Ankr API")
def fetch_holders(context, contract_address: str) -> DynamicOutput[pd.DataFrame]:
    url = os.getenv("ANKR_RPC_URL")  # e.g., https://rpc.ankr.com/multichain/<key>
    page_token = None

    while True:
        payload = {
            "id": 1,
            "jsonrpc": "2.0",
            "method": "ankr_getTokenHolders",
            "params": {"blockchain": "base", "contractAddress": contract_address, "pageSize": 10000}
        }
        if page_token:
            payload["params"]["pageToken"] = page_token

        resp = requests.post(url, json=payload)
        resp.raise_for_status()
        result = resp.json().get('result', {})
        holders = result.get('holders', [])
        if not holders:
            break

        df = pd.DataFrame(holders)
        context.log.info(f"Fetched {len(df)} holders for {contract_address}")

        yield DynamicOutput(df, mapping_key=f"{contract_address}_{page_token or 'first'}")

        page_token = result.get('nextPageToken')
        if not page_token:
            break

        time.sleep(1)

# Op: ingest one page of holders into Neo4j
@op(required_resource_keys={'neo4j'}, description="Ingest holder DataFrame into Neo4j")
def ingest_holders(context, holder_df: pd.DataFrame, contract_address: str):
    # Merge token node
    merge_token = f"MERGE (t:Token {{address: '{contract_address.lower()}'}})"
    context.resources.neo4j.run_query(merge_token)

    records = holder_df.to_dict('records')
    params_list = [
        {"address": r['holderAddress'].lower(), "balance": float(r['balance']), "balanceRaw": r['balanceRawInteger']}
        for r in records
    ]

    # Batch create relationships
    cypher = f"UNWIND $holders AS h\n"
    cypher += "MERGE (w:Wallet {address: h.address})\n"
    cypher += f"WITH w\nMATCH (t:Token {{address: '{contract_address.lower()}'}})\n"
    cypher += "MERGE (w)-[r:HOLDS]->(t)\n"
    cypher += "SET r.balance = toFloat(h.balance), r.balanceRaw = toFloat(h.balanceRaw), r.lastUpdated = datetime()"

    context.resources.neo4j.run_query(cypher, holders=params_list)
    context.log.info(f"Ingested {len(records)} holders into Neo4j for {contract_address}")

# Graph: dynamic mapping of tokens → pages → ingestion
@graph
def token_holder_pipeline():
    tokens = [t.strip().lower() for t in os.getenv('BASE_TOKENS', '').split(',') if t]
    for token in tokens:
        fetch_holders(token).map(
            lambda df: ingest_holders.alias(f"ingest_{token}")(df, token)
        )

# Definitions
defs = Definitions(
    graph_defs=[token_holder_pipeline],
    resources={
        'neo4j': neo4j_resource
    }
)
