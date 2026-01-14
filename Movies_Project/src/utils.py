# src/utils.py

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable

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


def create_dim_table(
   spark,
   df_source : SparkSession,          # Silver
   table_name,         # gold.dimension
   value,              # "value"
   primary_key,        # "primary_key"
   overwrite: bool = True 
):
   """
   Crée une table de dimension GOLD (Type 1/N)
   - Supprime et remplace ou Insère uniquement les nouvelles valeurs
   """
   if not spark.catalog.tableExists(table_name) or overwrite == True :
       (
           df_source
           .select(col(value))
           .distinct()
           .withColumn(primary_key, monotonically_increasing_id())
           .write
           .format("delta")
           .mode("overwrite")
           .saveAsTable(table_name)
       )
       return "Insertion OK"
   
   # merge incrémentale
   if overwrite == False : 
    dim_table = DeltaTable.forName(spark, table_name)
    new_values = (
        df_source
        .select(value)
        .distinct()
    )
    (
        dim_table.alias("d")
        .merge(
            new_values.alias("s"),
            f"d.{value} = s.{value}"
        )
        .whenNotMatchedInsert(
            values={
                primary_key: "monotonically_increasing_id()",
                value: f"s.{value}"
            }
        )
        .execute()
    )
    return "Merge OK"

from pyspark.sql.functions import col,split,explode,trim

def typage_data(df):
    """
    Nettoyage et typage des données TMDB.
    1. Supprime les lignes où title est NULL
    2. Cast vote_average en double
    3. Cast release_date en date   
    4. Cast vote_count en int
    5. Cast revenue en int
    6. Cast runtime en int
    7. Cast budget en int
    8. Cast adult en boolean
    9. Cast popularity en double
    """
  
    return (
        df
        .filter(col("title").isNotNull())
        .withColumn("vote_average", col("vote_average").cast("double"))
        .withColumn("release_date", col("release_date").cast("date"))
        .withColumn("vote_count", col("vote_count").cast("int"))
        .withColumn("revenue", col("revenue").cast("bigint"))
        .withColumn("runtime", col("runtime").cast("int"))
        .withColumn("budget", col("budget").cast("int"))
        .withColumn("adult", col("adult").cast("boolean"))
        .withColumn("popularity", col("popularity").cast("double"))

    )


def drop_columns(df, columns_to_drop : list):
    """
    Supprime les colonnes à enlever.
    - df: Spark DataFrame
    - columns_to_drop: list of columns to drop
    """
    return df.drop(*columns_to_drop)


def create_silver_multivalue(df_bronze, id_col : str, multivalue_col : str,new_col : str, separator=","):
    """
    Création d'un Spark DataFrame Silver à partir d'un Spark DataFrame Bronze.
    - df_bronze: Spark DataFrame Bronze
    - id_col: colonne id
    - new_col : nouvelle colonne
    - multivalue_col: colonne multivalue
    - separator: séparateur de la colonne multivalue
    """

    # 1. Séparer la colonne multivaluée en liste
    df_split = df_bronze.select(
    col(id_col),
    split(col(multivalue_col), separator).alias("values_list")
    )

    # 2. Exploser la liste pour obtenir une ligne par valeur
    df_exploded = df_split.select(
    col(id_col),
    explode(col("values_list")).alias(new_col)
    )
    # 3. Nettoyer la valeur (trim)
    df_clean = df_exploded.withColumn(
    new_col,
    trim(col(new_col))
    )
    # 4. Filtrer les valeurs nulles
    df_filtered = df_clean.filter(
    col(new_col).isNotNull()
    )

    return df_filtered


def create_dataframe_relationnel(
    df_table_relationnelle,
    df_table_dimension,
    column_relation : str, 
    column_dimension : str, 
    new_id_table_relationnelle :str, 
    new_id_table_dimension : str): 

    """
    Création d'une table relationnelle à partir d'une table dimension et de la table principale.
    - df_table_relationnelle: Spark DataFrame relationnelle
    - df_table_dimension: Spark DataFrame dimension
    - column_relation: colonne de la table relationnelle
    - column_dimension: colonne de la table dimension
    - new_id_table_relationnelle: nouvelle colonne de la table relationnelle
    - new_id_table_dimension: nouvelle colonne de la table dimension
    """
    res = (
    df_table_relationnelle.alias('table_relationnelle')
    .join(
        df_table_dimension.alias('table_dimension'),
        col('table_relationnelle.'+column_relation) == col('table_dimension.'+column_dimension)
    )
    .select(
        col('table_relationnelle.id').alias(new_id_table_relationnelle),
        col('table_dimension.id').alias(new_id_table_dimension)
    )
    )
    return res