import os

LEADER_DB = {
    "dbname": os.environ.get("LEADER_DB_NAME", "postgres"),
    "user": os.environ.get("LEADER_DB_USER", "postgres"),
    "password": os.environ.get("LEADER_DB_PASSWORD", "CENG465_Aliler"), 
    "host": os.environ.get("LEADER_DB_HOST", "10.0.0.4"),  # Leader IP address
    "port": os.environ.get("LEADER_DB_PORT", "5432")
}

FOLLOWER_DB = {
    "dbname": os.environ.get("FOLLOWER_DB_NAME", "postgres"),
    "user": os.environ.get("FOLLOWER_DB_USER", "postgres"),
    "password": os.environ.get("FOLLOWER_DB_PASSWORD", "CENG465_Aliler"), 
    "host": os.environ.get("FOLLOWER_DB_HOST", "10.0.0.5"),  # Follower IP address
    "port": os.environ.get("FOLLOWER_DB_PORT", "5432")
}