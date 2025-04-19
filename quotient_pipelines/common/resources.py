# quotient_pipelines/common/resources.py

import os
from typing import Any, Dict, List

import httpx
from dagster import resource, InitResourceContext
from neo4j import GraphDatabase, BoltDriver

from dotenv import load_dotenv

load_dotenv()

class Neo4jClient:
    """
    Wraps the Neo4j driver and exposes a simple run_query method.
    """
    def __init__(self, driver: BoltDriver):
        self._driver = driver

    def run_query(self, cypher: str, **parameters: Any) -> List[Dict[str, Any]]:
        """
        Execute a Cypher query and return a list of dicts (one per record).
        """
        with self._driver.session() as session:
            result = session.run(cypher, **parameters)
            return [record.data() for record in result]


@resource(
    description="Provides a Neo4jClient initialized from NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD",
)
def neo4j_resource(init_context: InitResourceContext) -> Neo4jClient:
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    pwd = os.getenv("NEO4J_PASS")
    driver = GraphDatabase.driver(uri, auth=(user, pwd))
    return Neo4jClient(driver)
