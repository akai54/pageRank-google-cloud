# large scale data management

Page rank in Pig, based on https://gist.github.com/jwills/2047314
Modified for running on Google Cloud Dataproc

This work with private directories on Google Storage.

We need to create them 

'''
gcloud storage buckets create gs://BUCKET_NAME --project=PROJECT_ID  --location=europe-west1 --uniform-bucket-level-access
'''
