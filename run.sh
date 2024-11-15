#!/bin/bash

# Paramètres du cluster
CLUSTER_NAME="cluster-a35a"
BUCKET_NAME="beshoux-large_bucket" 

# Crée le cluster avec le nombre de nœuds spécifié
gcloud dataproc clusters create $CLUSTER_NAME --enable-component-gateway --region europe-west1 --zone europe-west1-c --master-machine-type n2-standard-4 --master-boot-disk-size 50 --num-workers 4

# Copie des données dans le bucket GCS
gsutil cp small_page_links.nt gs://$BUCKET_NAME/

# Copie du code PySpark
gsutil cp PyPageRank.py gs://$BUCKET_NAME/

# Nettoie le répertoire de sortie
gsutil rm -rf gs://$BUCKET_NAME/out
gsutil rm -rf gs://small_page_links/out

# Exécute le job PySpark
gcloud dataproc jobs submit pyspark gs://$BUCKET_NAME/dataproc.py --region europe-west1 --cluster $CLUSTER_NAME

# Affiche les résultats
gsutil cat gs://$BUCKET_NAME/out/pagerank_data_10/part-r-00000

# Supprime le cluster pour éviter les frais supplémentaires
gcloud dataproc clusters delete $CLUSTER_NAME --region europe-west1 -q