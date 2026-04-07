import argparse
import sys
from pathlib import Path

from neo4j import GraphDatabase

_DAGS_ROOT = Path(__file__).resolve().parents[2]
if str(_DAGS_ROOT) not in sys.path:
    sys.path.insert(0, str(_DAGS_ROOT))


def ensure_indexes(uri, user, password):
    """
    Ensures that the necessary indexes and constraints are created in Neo4j
    for efficient data loading and querying. This function is idempotent.
    """
    driver = GraphDatabase.driver(uri, auth=(user, password))

    # Schema creation is idempotent. If the index or constraint already exists,
    # the command will not fail.
    queries = [
        # Constraints create a unique index automatically. Perfect for primary keys.
        "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Crime) REQUIRE c.case_number IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (d:Date) REQUIRE d.date_label IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (l:Location) REQUIRE l.location_text_key IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (ct:CrimeType) REQUIRE ct.crime_type_key IS UNIQUE",
    ]

    with driver.session() as session:
        for query in queries:
            print(f"Executing: {query}")
            session.run(query)

    print("Successfully created indexes and constraints.")
    driver.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ensure Neo4j indexes and constraints exist.")
    parser.add_argument("--neo4j_uri", required=True, help="Neo4j connection URI")
    parser.add_argument("--neo4j_user", required=True, help="Neo4j username")
    parser.add_argument("--neo4j_password", required=True, help="Neo4j password")
    args = parser.parse_args()

    ensure_indexes(args.neo4j_uri, args.neo4j_user, args.neo4j_password)
