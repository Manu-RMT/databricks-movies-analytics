# src/config.py

# ----------------------
# Catalog & Schemas
# ----------------------
CATALOG = "workspace"        # Nom du catalog Unity
RAW_SCHEMA = "datasets"          # Schema temporaire pour RAW
BRONZE_SCHEMA = "01_bronze"     # Schema pour Bronze (raw)
SILVER_SCHEMA = "02_silver"     # Schema pour Silver (Delta nettoyé)
GOLD_SCHEMA = "03_gold"         # Schema pour Gold (BI-ready)

# ----------------------
# Volume & fichiers
# ----------------------
VOLUME_PATH = "/Volumes/workspace/datasets/movies/"  # Volume contenant tous les CSV
CSV_EXTENSION = ".csv"                             # Extension des fichiers à lire

# ----------------------
# Tables Delta
# ----------------------
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.tmdb_movies"
SILVER_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.tmdb_movies"
GOLD_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.tmdb_movies_yearly"