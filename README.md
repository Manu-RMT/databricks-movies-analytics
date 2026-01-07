%md
# Projet TMDB Movies – Notes complètes

## Présentation du dataset TMDb

TMDb (The Movie Database) est une base de données de films qui fournit des informations détaillées, notamment :
- titres
- notes et évaluations
- dates de sortie
- revenus
- genres
- casting
- sociétés de production

Le dataset contient un échantillon de **1 000 000 de films** issus de TMDb.

---


## 1️⃣ Objectif du projet
L’objectif final du projet est de :

1. **Mettre en place un pipeline robuste** (Bronze → Silver → Gold) sur Databricks avec Delta Lake, capable de gérer :
   - un volume important de données,
   - des structures de texte complexes,
   - des mises à jour incrémentales,
   - l’historisation via Time Travel.

2. **Fournir un modèle de données en étoile** :
   - exploitable directement par les métiers,
   - compatible avec les outils de BI (Power BI, Tableau, etc.),
   - stable et extensible pour de futurs besoins.
   - performant pour l'analyse

3. **Construire un dashboard d’analyse** basé sur les tables Gold, permettant :
   - d’analyser la performance des films (budget, revenus, rentabilité),
   - de suivre les tendances par genre, période, studio,
   - d’identifier les films les mieux notés ou les plus populaires,
   - de répondre rapidement aux questions métiers sur le catalogue de films TMDb.


---

## 2️⃣ Structure du pipeline

| Couche   | Contenu                                                | Objectif                                         |
|----------|--------------------------------------------------------|-------------------------------------------------|
| **Bronze** | Données brutes de tous les CSV, toutes les colonnes   | Historisation complète, jamais modifiées       |
| **Silver** | Données nettoyées, typées, filtrées, merge incrémental | Source de vérité pour analyses et Gold         |
| **Gold**   | Modèle en étoile              | Dashboards et rapports BI optimisés            |

---

## 3️⃣ Étapes réalisées

### 3.1 Ingestion Bronze
- Lecture automatique de tous les CSV dans le volume `/Volumes/workspace/demo/movies/`.
- Options Spark pour gérer correctement les titres contenant des **virgules ou retours à la ligne** :
  - `header=True`
  - `multiLine=True`
  - `escape='"'`
  - `quote='"'`
  - `ignoreLeadingWhiteSpace=True`
- Fusion de tous les CSV avec `unionByName`.
- Écriture en Delta table Bronze :
  - `df_bronze.write.format("delta").mode("overwrite").saveAsTable(BRONZE_TABLE)`
- **Time Travel** possible pour revenir à n’importe quelle version précédente.

---

### 3.2 Transformation Silver
- Fonction `clean_tmdb(df)` :
  - Filtre `title IS NOT NULL`
  - Typage :
    - `vote_average` → double
    - `release_date` → date
- Gestion des **ID uniques** :
  - Si `id` absent, création automatique : `mon_id = concat(title, release_date)`
- Merge incrémental Delta pour ajouter uniquement les nouvelles lignes.
- Comptage du nombre de lignes insérées avant/après le merge pour vérifier les nouvelles données.

---

### 3.3 Création Gold
- Gold peut contenir :
  - Gestion des erreurs avec **try/except**
- Optimisation Delta pour BI :
  - `OPTIMIZE + ZORDER` pour accélérer les filtres et agrégations
- Solutions pour éviter les erreurs de schéma :
  - Supprimer la table Gold avant création (`DROP TABLE IF EXISTS`)
  - Ou `overwriteSchema=True` pour forcer la compatibilité
- Création des tables de fait et de dimensions : 
  - `Faits_Films` : Contient les mesures clés par film, par date de sortie, par genre, etc.
  - `Dim_Film` : informations descriptives du film (titre, langue, synopsis, statut).
  - `Dim_Date` : calendrier analytique (jour, mois, année, trimestre, jour de semaine).
  - `Dim_Genre` : genres de films (Action, Comédie, Drame, etc.).
  - `Dim_Production` : studios et pays de production.
  - `Dim_Casting` : acteurs principaux, réalisateurs, scénaristes.
  - `Dim_Collection` : sagas / franchises (ex. : Star Wars, Harry Potter).


---




### 3.5 Bonnes pratiques et recommandations
- **Bronze = raw**, jamais modifié.
- **Silver = nettoyé, typé, source de vérité**, idéal pour créer des **dimensions et tables fact**
- **Gold = BI-ready**, tables détaillées ou agrégées, optimisées pour dashboards.
- Créer des **dimensions** : `dim_movie`, `dim_genre`, `dim_language`.
- Créer des **tables fact** : `fact_movie_rating` pour votes et notes.
- Utiliser **merge incrémental** pour ajouter seulement les nouvelles données.
- Utiliser **Time Travel** pour revenir à une version précédente si nécessaire.
- Optimiser Gold avec `OPTIMIZE + ZORDER` pour des filtres rapides.

---

### 3.6 Gestion des CSV problématiques
- Titres avec **virgules, guillemets ou retours à la ligne** ne cassent plus le pipeline grâce aux options Spark.
- Colonnes correctement remises dans l’ordre après lecture.
- Silver et Gold ne perdent aucune donnée essentielle.

---

### 3.7 Avantages de ce pipeline
- **Fiable et robuste** : merge incrémental, gestion des IDs uniques, Time Travel
- **Extensible** : possibilité de créer d’autres dimensions ou KPIs
- **BI-ready** : tables Gold optimisées pour dashboards
- **Pro** : conforme aux bonnes pratiques Data Lake / Data Warehouse sur Databricks

---

## 4️⃣ Recommandation globale
- **Bronze** → données brutes
- **Silver** → données nettoyées et typées, dimensions et faits
- **Gold** → tables BI-ready, détaillées ou agrégées
- **Time Travel Delta** → revenir à une version précédente
- **Merge incrémental** → ajouter uniquement les nouvelles lignes
