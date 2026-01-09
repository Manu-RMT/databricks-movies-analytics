# src/utils.py

import sys
from pyspark.sql import SparkSession

# -----------------------------
# Ajouter le repo au Python Path
# -----------------------------
sys.path.append("/Workspace/Users/mandu543@gmail.com/databricks-movies-analytics/Movies_Project")

# -----------------------------
# Importer la config et transformations
# -----------------------------
from src.config import *
from src.transformations import *

# -----------------------------
# Fonctions utilitaires
# -----------------------------

def show_schemas(spark: SparkSession):
    """Afficher tous les schemas du catalog 'workspace'"""
    spark.sql("SHOW SCHEMAS IN workspace").show()

def create_schema_if_not_exists(spark: SparkSession, table_name: str):
    """Créer le schema si inexistant à partir d'un nom de table complet"""
    schema_name = table_name.split('.')[1] if '.' in table_name else table_name
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

def save_dataframe(df: SparkSession, table_name: str, mode: str = "overwrite"):
    """Sauvegarder un dataframe dans un table"""
    df.write.format("delta").mode(mode).saveAsTable(table_name)
