# quotient_pipelines/common/metadata.py

import os
import httpx
import requests
from dagster import op, DynamicOut, DynamicOutput

from dotenv import load_dotenv

load_dotenv()

@op(
    out=DynamicOut(),
    description="Fetch ERC20 token metadata from Moralis (reusable in any pipeline)",
)
def fetch_moralis_token_metadata(context, addresses: list[str]):
    """
    Fetch token metadata (name, symbol, marketCap, etc.) from Moralis for each address.
    Yields DynamicOutput with a dict containing at least {'address': ..., ...}, mapping_key=address.
    """
    api_key = os.getenv("MORALIS_API_KEY")
    headers = {"X-API-Key": api_key}
    url = os.getenv(
        "MORALIS_METADATA_URL", "https://deep-index.moralis.io/api/v2.2/erc20/metadata"
    )
    chain = "base"
    
    for address in addresses:
        context.log.info(f"Fetching metadata for {address} via Moralis")
        try:
            resp = httpx.get(
                url,
                headers=headers,
                params={"chain": chain, "addresses": address},
            )
            resp.raise_for_status()
            items = resp.json()
            # Moralis returns a list; take the first element
            data = items[0] if isinstance(items, list) and items else {}
        except Exception as err:
            context.log.error(f"Error fetching metadata for {address}: {err}")
            continue

        token_meta = {
            "address": address,
            "name": data.get("name"),
            "symbol": data.get("symbol"),
            "marketCap": data.get("market_cap"),
            # add more fields as needed
        }

        yield DynamicOutput(token_meta, mapping_key=address)


@op(
    out=DynamicOut(),
    description="Fetch # of holders for token from Ankr",
)
def get_token_holders_count_ankr(context, token_address):
    """Get token holder count from Ankr API"""
    url = os.getenv('ANKR_RPC_URL')
    
    payload = {
        "jsonrpc": "2.0",
        "method": "ankr_getTokenHoldersCount",
        "params": {
            "blockchain": "base",
            "contractAddress": token_address
        },
        "id": 1
    }
    
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        token_count_dict = {
            'address': token_address,
            'holderCount': data['result']['holderCountHistory'][0]['holderCount']
        }
        context.log.info(f"Collected token holder count for token: {token_address}:  {token_count_dict})")
        yield DynamicOutput(token_count_dict, mapping_key=token_address)
    
    except Exception as e:
        context.log.error(f"Error fetching holder count for {token_address}: {e}")
        return None

