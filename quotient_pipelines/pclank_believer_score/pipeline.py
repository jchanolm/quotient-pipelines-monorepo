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
from quotient_pipelines.common.metadata import fetch_moralis_token_metadata
from quotient_pipelines.common.holders import fetch_and_ingest_holders

from .believer_score_query import believer_score_query

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



@op(required_resource_keys={"neo4j"})
def set_pclank_believer_scores(context):
    context.log.info(f"Running believer score query...")
    ### add decorators for head + counts
    query_txt = believer_score_query()
    believer_query_response = context.resources.neo4j.run_query(query_txt)
    context.log.info(f"Results from believer query: {believer_query_response}")
    context.log.info(f"Believer scores updated successfully")



@graph
def pclank_believer_score_graph():
    # First part: Get metadata and set it in Neo4j
    # addrs = list_addresses()
    # metas = fetch_moralis_token_metadata(addrs)
    # metas.map(set_token_metadata)
    
    # Set believer scores
    set_pclank_believer_scores()

# ─── 5) Expose as a Job & Definitions ─────────────────────────────────────────
pclank_believer_score_job = pclank_believer_score_graph.to_job(
    name="pclank_believer_score",
    resource_defs={"neo4j": neo4j_resource},
)

defs = Definitions(jobs=[pclank_believer_score_job])