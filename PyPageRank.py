#!/usr/bin/env python
# coding: utf-8

# <a href="https://colab.research.google.com/github/momo54/large_scale_data_management/blob/main/PyPageRank.ipynb" target="_parent"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/></a>

# In[1]:


#!pip install pyspark


# In[2]:


#!pip install -q findspark
import findspark
findspark.init()


# # SPARK INSTALLED... lets play

# In[3]:


from pyspark.sql import SparkSession
spark = SparkSession.builder\
        .master("local")\
        .appName("Colab")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()


# In[4]:


#!wget -q https://storage.googleapis.com/public_lddm_data/small_page_links.nt
get_ipython().system('ls')


# In[5]:


lines = spark.read.text("gs://beshoux-large_bucket/page_links_en.nt.bz2").rdd.map(lambda r: r[0])
lines.take(5)


# In[6]:


import re
def computeContribs(urls, rank) :
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls) :
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[2]


# In[7]:


# Loads all URLs from input file and initialize their neighbors.
links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

# Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))


# In[8]:


links.take(5)


# In[9]:


#groupByKey makes lists !!
links.map(lambda x: (x[0],list(x[1]))).take(5)


# In[10]:


#groupByKey makes lists !!


# In[ ]:


ranks.take(5)


# In[ ]:


links.join(ranks).take(5)


# In[ ]:


links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(
            url_urls_rank[1][0], url_urls_rank[1][1]  # type: ignore[arg-type]
        )).take(5)


# In[ ]:


from operator import add
for iteration in range(1):
  # Calculates URL contributions to the rank of other URLs.
  contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(
            url_urls_rank[1][0], url_urls_rank[1][1]  # type: ignore[arg-type]
        ))

  # Re-calculates URL ranks based on neighbor contributions.
  ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    # Collects all URL ranks and dump them to console.
for (link, rank) in ranks.collect():
  print("%s has rank: %s." % (link, rank))


# # --- Implémentation avec PySpark RDD ---

# In[ ]:


# Fonction pour parser les voisins (identique à celle utilisée dans DataFrame)
def parseNeighborsRDD(urls):
    parts = re.split(r'\s+', urls)
    return parts[0], parts[2]

# Fonction pour calculer les contributions de rang (identique à celle utilisée dans DataFrame)
def computeContribsRDD(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


# In[ ]:


# Chargement des données et initialisation avec RDD
lines_rdd = spark.read.text("page_links_en.nt.bz2").rdd.map(lambda r: r[0])  # Remplace par ton chemin

# Initialisation des liens et des rangs avec RDD
links_rdd = lines_rdd.map(lambda urls: parseNeighborsRDD(urls)).distinct().groupByKey().cache()
ranks_rdd = links_rdd.map(lambda url_neighbors: (url_neighbors[0], 1.0))


# In[ ]:


# Boucle d'itérations pour le calcul de PageRank avec RDD
num_iterations = 10
for iteration in range(num_iterations):
    contribs_rdd = links_rdd.join(ranks_rdd).flatMap(lambda url_urls_rank: computeContribsRDD(
        url_urls_rank[1][0], url_urls_rank[1][1]
    ))
    
    ranks_rdd = contribs_rdd.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)


# In[ ]:


print("Résultats PageRank avec RDD:")
for (link, rank) in ranks_rdd.collect():
    print("%s has rank: %s." % (link, rank))

