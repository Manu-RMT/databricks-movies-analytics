# src/transformations.py

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

