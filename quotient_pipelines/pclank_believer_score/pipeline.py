import os
import httpx
from dagster import (
    op,
    graph,
    DynamicOut,
    DynamicOutput,
    Definitions,
)

from quotient_pipelines.common.resources import neo4j_resource
from quotient_pipelines.common.metadata import fetch_moralis_token_metadata, get_token_holders_count_ankr
from quotient_pipelines.common.holders import fetch_and_ingest_holders
from quotient_pipelines.common.neo4j_ingestor import Neo4jIngestor  # Import the resource function, not the class

from .believer_score_query import believer_score_query

# ─── 1) Check Product Clank for Tokens ─────────────────────────────────────────────
@op(required_resource_keys={"neo4j"})
def fetch_and_merge_pclank_tokens(context):
    """
    Fetch tokens from Product Clank API and merge them into Neo4j
    """
    PCLANK_API_URL = "https://app.productclank.com/api/getTokens"
    API_KEY = "vTIWAa$F1nm6Qz"
    
    try:
        # Fetch tokens from Product Clank
        headers = {"x-api-key": API_KEY}
        response = httpx.get(PCLANK_API_URL, headers=headers)
        response.raise_for_status()
        
        tokens_data = response.json()
        tokens = tokens_data.get("tokens", [])
        
        # Merge each token into Neo4j
        for token in tokens:
            address = token["address"].lower()  # Normalize address to lowercase
            ticker = token["ticker"]
            
            cypher = """
            MERGE (t:Token {address: $address})
            ON CREATE SET 
                t.ticker = $ticker,
                t.createdDt = datetime(),
                t.lastUpdateDt = datetime(),
                t.source = 'product_clank'
            ON MATCH SET 
                t.ticker = $ticker,
                t.lastUpdateDt = datetime(),
                t.source = 'product_clank'
            """
            
            context.resources.neo4j.run_query(
                cypher,
                address=address,
                ticker=ticker
            )
            
        context.log.info(f"Successfully merged {len(tokens)} tokens from Product Clank")
        return [token["address"].lower() for token in tokens]
        
    except Exception as e:
        context.log.error(f"Error fetching/merging Product Clank tokens: {str(e)}")
        return []

# ─── 1) Pull addresses from Neo4j ─────────────────────────────────────────────
@op(required_resource_keys={"neo4j"})
def list_addresses(context) -> list[str]:
    """
    Pull a distinct list of token contract addresses from Neo4j.
    """
    cypher = """
    MATCH (t:Token)
    RETURN DISTINCT t.address AS address
    """
    neo4j_client = context.resources.neo4j  # This is the Neo4jClient instance
    result = neo4j_client.run_query(cypher)
    tokens = [record['address'] for record in result]
    context.log.info(f"Found {len(tokens)} tokens in Neo4j: {tokens}")
    return tokens

# Create a dynamic output for addresses
@op(out=DynamicOut())
def process_addresses(context, addresses: list[str]):
    """Convert a list of addresses into individual dynamic outputs"""
    for i, addr in enumerate(addresses):
        yield DynamicOutput(addr, mapping_key=f"addr_{i}")

# ─── 2) Set Token Metadata  ────────────────────────
@op(required_resource_keys={"neo4j"})
def set_token_metadata(context, metadata: dict):
    """
    Set token metadata on :Token node in Neo4j
    """
    address = metadata.get('address').lower()
    name = metadata.get('name')
    symbol = metadata.get('symbol')
    marketCap = metadata.get('marketCap')

    cypher = """
    MERGE (t:Token {address:$address})
    ON CREATE SET 
        t.name = $name, 
        t.symbol = $symbol, 
        t.marketCap = tofloat($marketCap),
        t.createdDt = timestamp(),
        t.lastUpdateDt = timestamp()
    ON MATCH SET
        t.name = $name, 
        t.symbol = $symbol, 
        t.marketCap = tofloat($marketCap),
        t.lastUpdateDt = timestamp()
    """

    context.resources.neo4j.run_query(
        cypher,
        address=address,
        name=name,
        symbol=symbol,
        marketCap=marketCap
    )
    context.log.info(f"Upserted Token({address}) → name={name}, symbol={symbol}, marketCap={marketCap}")


@op(required_resource_keys={'neo4j'})
def set_token_holder_count(context, token_data: dict):
    """
    Sets holder count on :Token.holderCount using data from Ankr API
    """
    try:
        address = token_data['address'].lower()
        holder_count = token_data['holderCount']
        
        cypher = """
        MATCH (t:Token {address: $address})
        SET t.holderCount = $holder_count,
            t.lastUpdateDt = datetime()
        RETURN t.address, t.holderCount
        """
        
        result = context.resources.neo4j.run_query(
            cypher,
            address=address,
            holder_count=holder_count
        )
        
        context.log.info(f"Updated holder count for {address} to {holder_count}")
        return result
    except Exception as e:
        context.log.error(f"Error setting holder count for {token_data['address']}: {str(e)}")
        return None


@op(required_resource_keys={"neo4j"})
def set_pclank_believer_scores(context, metadata_results=None, holder_count_results=None, holder_ingestion_results=None):
    """
    Calculate and set believer scores in Neo4j.
    The parameters are only used to create dependencies, ensuring this runs after metadata and holder count operations.
    """
    context.log.info(f"All metadata, holder count, and holder ingestion operations complete. Starting believer score calculation...")
    query_txt = believer_score_query()
    believer_query_response = context.resources.neo4j.run_query(query_txt)
    context.log.info(f"Results from believer query: {believer_query_response}")
    context.log.info(f"Believer scores updated successfully")


@graph
def pclank_believer_score_graph():
    # First: Fetch and merge Product Clank tokens
    fetch_and_merge_pclank_tokens()
    
    # Then get all addresses from Neo4j
    addrs = list_addresses()
    
    # Get and set token metadata
    metas = fetch_moralis_token_metadata(addrs)
    metadata_results = metas.map(set_token_metadata)
    
    # # Get and set holder counts for each token
    holder_counts = get_token_holders_count_ankr(addrs)
    holder_count_results = holder_counts.map(set_token_holder_count)
    
    # ADDED: Process addresses for holder ingestion using S3 and Neo4j
    # addr_outputs = process_addresses(addrs)
    # holder_ingestion_results = addr_outputs.map(fetch_and_ingest_holders)
    
    # Only run set_pclank_believer_scores after all operations are complete
    set_pclank_believer_scores(
        metadata_results.collect(),
        holder_count_results.collect(),
    )


# ─── 5) Expose as a Job & Definitions ─────────────────────────────────────────
pclank_believer_score_job = pclank_believer_score_graph.to_job(
    name="pclank_believer_score",
    resource_defs={
        "neo4j": neo4j_resource,
        "neo4j_ingestor": Neo4jIngestor
    },
)

defs = Definitions(jobs=[pclank_believer_score_job])