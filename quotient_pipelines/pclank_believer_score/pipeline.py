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
from quotient_pipelines.common.holders import fetch_holders

TEST_ADDRESS = "0x0ce2495d150daf8a3cd2f5200c4c2694e2934c1a"

@op 
def print_df(context, df):
    context.log.info(f"Page rows = {len(df)}")
    context.log.debug(df.head)

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

# ─── 2) Compute & Store  ────────────────────────
@op(required_resource_keys={"neo4j"})
def compute_and_store_neo4j(context, metadata: dict):
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
        t.marketCap = $marketCap,
        t.createdDt = timestamp(),
        t.lastUpdateDt = timestamp()
    ON MATCH SET
        t.marketCap = $marketCap,
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

@graph
def pclank_believer_score_graph():
    addrs = list_addresses()
    metas = fetch_moralis_token_metadata(addrs)
    metas.map(compute_and_store_neo4j)


# ─── 5) Expose as a Job & Definitions ─────────────────────────────────────────
pclank_believer_score_job = pclank_believer_score_graph.to_job(
    name="pclank_believer_score",
    resource_defs={"neo4j": neo4j_resource},
)

defs = Definitions(jobs=[pclank_believer_score_job])

