# quotient_pipelines/common/metadata.py

import os
import httpx
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

