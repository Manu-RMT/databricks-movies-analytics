# src/transformations.py

from pyspark.sql.functions import col

def clean_tmdb(df):
    """
    Nettoyage et typage des données TMDB.
    1. Supprime les lignes où title est NULL
    2. Cast vote_average en double
    3. Cast release_date en date
    """
    return (
        df
        .filter(col("title").isNotNull())
        .withColumn("vote_average", col("vote_average").cast("double"))
        .withColumn("release_date", col("release_date").cast("date"))
    )
