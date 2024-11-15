import subprocess
import sys

def install_packages():
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pyspark"])
    subprocess.check_call([sys.executable, "-m", "pip", "install", "findspark"])

if __name__ == "__main__":
    install_packages()

    import findspark
    import time
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit, coalesce, sum as spark_sum
    from pyspark.sql.types import StructType, StructField, StringType

    if len(sys.argv) != 3:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)

    findspark.init()

    # Création d'une session Spark
    spark = SparkSession.builder.appName("PageRankExample").getOrCreate()

    # Définition du schéma pour les données RDF
    schema = StructType([
        StructField("source", StringType(), nullable=True),
        StructField("predicate", StringType(), nullable=True),
        StructField("target", StringType(), nullable=True)
    ])

    # Chargez les données dans le DataFrame
    data = spark.read.option("delimiter", " ").csv(sys.argv[1], header=False, schema=schema)

    # Créez un DataFrame contenant le nombre de liens sortants pour chaque page
    outdegrees = data.groupBy("source").count().withColumnRenamed("source", "page").withColumnRenamed("count", "outDegree")

    # Initialisation du PageRank à 1.0 pour chaque page
    initial_pagerank = 1.0
    all_pages = data.select("source").union(data.select("target")).distinct().withColumnRenamed("source", "page")
    pagerank = all_pages.withColumn("pagerank", lit(initial_pagerank))
    damping_factor = 0.85

    # Nombre d'itérations à partir de l'argument
    max_iterations = int(sys.argv[2])
    debut = time.time()

    # Effectuez des itérations pour calculer le PageRank
    for iteration in range(max_iterations):
        data_alias = data.alias("data")
        pagerank_alias = pagerank.alias("pagerank")
        outdegrees_alias = outdegrees.alias("outdegrees")
        
        # Rejoignez les liens avec les rangs pour calculer les contributions
        links_with_ranks = data_alias \
            .join(pagerank_alias, col("data.source") == col("pagerank.page"), "left") \
            .join(outdegrees_alias, col("data.source") == col("outdegrees.page"), "left")
        
        # Calcul des contributions de chaque lien, en gérant les nulls avec coalesce
        contribs = links_with_ranks.withColumn("contrib", coalesce(col("pagerank.pagerank") / col("outdegrees.outDegree"), lit(0.0))) \
                                   .select(col("data.target").alias("page"), "contrib")

        # Calcul du nouveau PageRank en sommant les contributions
        new_pagerank = contribs.groupBy("page").agg(spark_sum("contrib").alias("pagerank"))

        # Assurez-vous que chaque page conserve un pagerank, même sans contributions
        pagerank = all_pages.join(new_pagerank, "page", "left") \
                            .withColumn("pagerank", coalesce(col("pagerank"), lit(0.0))) \
                            .withColumn("pagerank", (1 - damping_factor) + damping_factor * col("pagerank"))

    # Affichez les résultats
    pagerank.select("page", "pagerank").show()

    finB = time.time()  # Capture du temps avant le calcul du max

    # Obtenez la page avec le rang maximum de manière optimisée avec orderBy
    max_rank_row = pagerank.orderBy(col("pagerank").desc()).first()
    max_rank_page = max_rank_row["page"]
    max_rank = max_rank_row["pagerank"]

    print(f"Page avec le rang maximum : {max_rank_page} avec un rang de : {max_rank}.")

    fin = time.time()
    print(f"Temps d'exécution : {fin - debut} secondes")
    print(f"Temps d'exécution sans max: {finB - debut} secondes")

    # Arrêtez la session Spark
    spark.stop()