import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

from neo4j import GraphDatabase

# Add project root to Python path
_DAGS_ROOT = Path(__file__).resolve().parents[2]
if str(_DAGS_ROOT) not in sys.path:
    sys.path.insert(0, str(_DAGS_ROOT))


def prune_old_data(uri, user, password, retention_days=365):
    """
    Deletes Crime and Date nodes from Neo4j that are older than the specified
    retention period. It also cleans up any dangling Location or CrimeType
    nodes that are no longer connected to any crimes.
    """
    driver = GraphDatabase.driver(uri, auth=(user, password))

    cutoff_date = date.today() - timedelta(days=retention_days)
    cutoff_date_str = cutoff_date.isoformat()

    # This query is designed to be run incrementally. It will:
    # 1. Find all Date nodes older than the cutoff date.
    # 2. For each old Date, find all connected Crime nodes.
    # 3. Detach and delete those Crime nodes.
    # 4. After the crimes are gone, delete the old Date node itself.
    # 5. The `LIMIT 10000` makes the transaction manageable and prevents out-of-memory errors.
    #    Run this script multiple times if you have a large backlog of old data to delete.
    prune_crimes_and_dates_query = f"""
    MATCH (d:Date)
    WHERE d.date_label < '{cutoff_date_str}'
    WITH d LIMIT 10000
    MATCH (c:Crime)-[:OCCURRED_ON]->(d)
    DETACH DELETE c
    WITH d
    DETACH DELETE d
    RETURN count(d) AS deleted_dates
    """

    # This query cleans up any Location or CrimeType nodes that are no longer
    # connected to any crimes after the pruning operation.
    cleanup_dangling_nodes_query = """
    MATCH (n)
    WHERE (n:Location OR n:CrimeType) AND NOT (n)--()
    WITH labels(n) AS label, count(n) AS deleted_nodes
    MATCH (n)
    WHERE (n:Location OR n:CrimeType) AND NOT (n)--()
    DETACH DELETE n
    RETURN label, deleted_nodes
    """

    with driver.session() as session:
        print(f"Pruning data older than {cutoff_date_str}...")
        result = session.run(prune_crimes_and_dates_query)
        deleted_count = result.single()["deleted_dates"]
        print(f"Deleted {deleted_count} old Date nodes and their associated Crimes.")

    with driver.session() as session:
        print("Cleaning up dangling Location and CrimeType nodes...")
        try:
            result = session.run(cleanup_dangling_nodes_query)
            for record in result:
                print(
                    f"Deleted {record['deleted_nodes']} dangling {record['label'][0]} nodes."
                )
        except neo4j.exceptions.ClientError as e:
            print(f"Warning: Could not clean up dangling nodes. This can happen in rare race conditions and is safe to ignore. Error: {e}")

    driver.close()
    print("Successfully pruned old data.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prune old data from Neo4j.")
    parser.add_argument("--neo4j_uri", required=True, help="Neo4j connection URI")
    parser.add_argument("--neo4j_user", required=True, help="Neo4j username")
    parser.add_argument("--neo4j_password", required=True, help="Neo4j password")
    parser.add_argument(
        "--retention_days",
        type=int,
        default=365,
        help="Number of days of data to retain.",
    )
    args = parser.parse_args()

    prune_old_data(
        args.neo4j_uri,
        args.neo4j_user,
        args.neo4j_password,
        args.retention_days,
    )
