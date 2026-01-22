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
       return f"Insertion table :   {table_name} OK"
   
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
    return f"Merge table : {table_name} OK"

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
        .withColumn("id",col("id").cast("bigint"))
        .withColumn("vote_average", col("vote_average").cast("double"))
        .withColumn("release_date", col("release_date").cast("date"))
        .withColumn("vote_count", col("vote_count").cast("int"))
        .withColumn("revenue", col("revenue").cast("bigint"))
        .withColumn("runtime", col("runtime").cast("int"))
        .withColumn("budget", col("budget").cast("int"))
        .withColumn("adult", col("adult").cast("boolean"))
        .withColumn("popularity", col("popularity").cast("double"))

    )


from pyspark.sql.functions import when, lit
from pyspark.sql.types import (
   StringType, IntegerType, LongType, DoubleType, FloatType,
   DecimalType, ShortType, ByteType,
   DateType, TimestampType
)

def normalize_nulls(df):
    """Remplace les valeurs NULL par des valeurs par défaut"""
    for field in df.schema.fields:
        c = field.name
        t = field.dataType
        # Strings → ''
        if isinstance(t, StringType):
            df = df.withColumn(c, when(col(c).isNull(), lit("")).otherwise(col(c)))
        # Numériques → 0
        elif isinstance(t, (IntegerType, LongType, DoubleType, FloatType,
                            DecimalType, ShortType, ByteType)):
            df = df.withColumn(c, when(col(c).isNull(), lit(0)).otherwise(col(c)))
        # Dates → valeur par défaut (optionnel)
        elif isinstance(t, DateType):
            df = df.withColumn(c, when(col(c).isNull(), lit("1900-01-01")).otherwise(col(c)))
        elif isinstance(t, TimestampType):
            df = df.withColumn(c, when(col(c).isNull(), lit("1900-01-01 00:00:00")).otherwise(col(c)))
    return df


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

from typing import List, Optional
from pyspark.sql.utils import AnalysisException

def insert_new_rows(
   spark: SparkSession,
   df_new,
   table_name: str,
   key_columns: Optional[List[str]] = None
) -> str:
   """
   Insère uniquement les nouvelles lignes dans une table Spark/Delta.
   Si la table n'existe pas, elle est créée et toutes les lignes sont insérées.
   :param spark: SparkSession
   :param df_new: DataFrame contenant les nouvelles données
   :param table_name: nom de la table cible
   :param key_columns: liste des colonnes de comparaison
   :return: message avec le nombre de lignes insérées
   """
   if key_columns is None:
       key_columns = df_new.columns
   # Suppression des doublons côté source
   df_new_dedup = df_new.dropDuplicates(key_columns)

   try:
       # Vérifie si la table existe
       df_existing = spark.table(table_name)
       # Garde uniquement les nouvelles lignes
       df_to_insert = df_new_dedup.join(
           df_existing,
           on=key_columns,
           how="left_anti"
       )
       # Vérifie si le DataFrame à insérer est vide
       # `rdd.isEmpty()` est plus performant que df.count() pour les gros DataFrames,
       # car il arrête le calcul dès qu'une ligne est trouvée
       # if not df_to_insert.rdd.isEmpty(): => Pas compatible Databricks serverless
       if df_to_insert.limit(1).count() > 0:
           df_to_insert.write \
               .option("mergeSchema", "true") \
               .mode("append") \
               .saveAsTable(table_name)
           nb_inserted = df_to_insert.count()  # comptage après écriture
           return f"NB lignes merge {nb_inserted}"
       else:
           return "Aucune nouvelle ligne à insérer"
   except AnalysisException:
       # Table inexistante → création + insertion complète
       df_new_dedup.write \
           .option("mergeSchema", "true") \
           .mode("overwrite") \
           .saveAsTable(table_name)
       nb_inserted = df_new_dedup.count()
       return f"NB lignes insérées {nb_inserted}"

    

def getTable(spark, schema:str ,table_name:str):
    """
    Récupère la table Spark à partir du nom de la table.
    :param spark: SparkSession
    :param schema: nom du schéma
    :param table_name: nom de la table
    """
    return spark.table(schema+"."+table_name)
 
def test():
    df_new_dedup.createOrReplaceTempView("temp_new")
    df_existing.createOrReplaceTempView("temp_existing")
    return (
        df_new_dedup,
        df_existing)
    query = f"""
    SELECT n.*
    FROM temp_new n
    LEFT JOIN temp_existing e
    ON {" AND ".join([f"n.{c} = e.{c}" for c in key_columns])}
    """
    df_to_insert = spark.sql(query)
    return df_to_insert